import hashlib
import ipaddress
import json
import logging
import math
import os
import re
import socket
from contextlib import closing
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import Body, Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, field_validator


# ============================================================
# SolveEase 300-Target Task Engine
# Version 2.0.2 — Neon PostgreSQL
#
# API manages targets, queue visibility and manual feed scans.
# worker.py owns continuous feed polling.
# consumer.py owns task execution.
# ============================================================

APP_NAME = "SolveEase 300-Target Task Engine"
APP_VERSION = "2.0.2"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ENGINE_API_KEY = os.getenv("ENGINE_API_KEY", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

AUTO_SEED_TARGETS = os.getenv("AUTO_SEED_TARGETS", "false").lower() == "true"
ENABLE_API_BACKGROUND_SCANNER = (
    os.getenv("ENABLE_API_BACKGROUND_SCANNER", "false").lower() == "true"
)

SCAN_TIMEOUT_SECONDS = float(os.getenv("SCAN_TIMEOUT_SECONDS", "15"))
MAX_TASKS_PER_TARGET = int(os.getenv("MAX_TASKS_PER_TARGET", "100"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("solveease.main")

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Target management and task queue API.",
)


# ============================================================
# DATABASE
# ============================================================

def connect_database():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")

    clean_url = DATABASE_URL.strip().strip("'").strip('"')
    if "sslmode=" not in clean_url:
        separator = "&" if "?" in clean_url else "?"
        clean_url += f"{separator}sslmode=require"

    return psycopg.connect(
        clean_url,
        connect_timeout=10,
        row_factory=dict_row,
    )


def database_error(operation: str, exc: Exception):
    detail = str(exc)

    # Redact common database URL credentials.
    detail = re.sub(
        r"(?i)(postgres(?:ql)?://)[^@\s]+@",
        r"\1[REDACTED]@",
        detail,
    )
    detail = re.sub(
        r"(?i)\b(password|passwd|pwd)\s*=\s*[^;\s]+",
        r"\1=[REDACTED]",
        detail,
    )

    logger.error(
        "%s failed: exception=%s SQLSTATE=%s detail=%s",
        operation,
        type(exc).__name__,
        getattr(exc, "sqlstate", None),
        detail[:300],
    )


def require_database():
    try:
        with closing(connect_database()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception as exc:
        database_error("Database availability check", exc)
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check DATABASE_URL and Neon status.",
        ) from None


# ============================================================
# AUTHENTICATION
# Supports X-API-Key or Authorization: Bearer <key>
# ============================================================

def verify_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    import hmac

    if not ENGINE_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="API authentication is not configured.",
        )

    supplied = x_api_key or ""

    if authorization and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()

    if not supplied or not hmac.compare_digest(supplied, ENGINE_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")

    return True


# ============================================================
# INPUT VALIDATION
# ============================================================

def validate_endpoint_url(value: str, method: str) -> str:
    value = value.strip()
    parsed = urlparse(value)

    allowed_scheme = "wss" if method == "websocket" else "https"

    if parsed.scheme.lower() != allowed_scheme:
        raise ValueError(f"{method} targets must use {allowed_scheme}://")

    if not parsed.hostname:
        raise ValueError("Endpoint must include a hostname")

    if parsed.username or parsed.password:
        raise ValueError("Credentials must not be embedded in endpoint URLs")

    if parsed.fragment:
        raise ValueError("Endpoint URLs must not contain fragments")

    host = parsed.hostname.lower().rstrip(".")

    if host in {"localhost", "localhost.localdomain"}:
        raise ValueError("Localhost endpoints are not allowed")

    try:
        ip = ipaddress.ip_address(host)
        if not ip.is_global:
            raise ValueError("Private, loopback and reserved IPs are not allowed")
    except ValueError as exc:
        if "not allowed" in str(exc):
            raise
        if ":" in host:
            raise ValueError("Invalid endpoint hostname") from None

    return value


class TargetRegistration(BaseModel):
    platform_name: str = Field(min_length=2, max_length=120)
    api_endpoint: str = Field(min_length=8, max_length=2000)
    extraction_method: str = "httpx"
    tier: int = Field(default=2, ge=1, le=2)
    is_active: bool = False

    @field_validator("platform_name")
    @classmethod
    def clean_platform_name(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("platform_name cannot be blank")
        return value

    @field_validator("extraction_method")
    @classmethod
    def check_method(cls, value):
        value = value.strip().lower()
        if value not in {"httpx", "playwright", "websocket"}:
            raise ValueError("Use httpx, playwright or websocket")
        return value

    @field_validator("api_endpoint")
    @classmethod
    def check_endpoint(cls, value, info):
        method = info.data.get("extraction_method", "httpx")
        return validate_endpoint_url(value, method)


class TargetStatusUpdate(BaseModel):
    is_active: bool


# ============================================================
# TELEGRAM NOTIFICATIONS
# ============================================================

def send_telegram_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram not configured; alert skipped.")
        return False

    url = (
        "https://api.telegram.org/bot"
        + TELEGRAM_BOT_TOKEN
        + "/sendMessage"
    )

    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message[:3500],
                },
            )
            response.raise_for_status()

        return True
    except Exception as exc:
        logger.warning("Telegram notification failed: %s", type(exc).__name__)
        return False


# ============================================================
# SAFE FEED PARSING
# ============================================================

def extract_task_list(data):
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("tasks")
        if items is None:
            items = data.get("data")
        if items is None:
            items = data.get("results")
    else:
        return []

    if not isinstance(items, list):
        return []

    return [item for item in items[:MAX_TASKS_PER_TARGET]
            if isinstance(item, dict)]


def normalize_task(item: dict, platform_name: str):
    payout_value = item.get("payout_usd", item.get("payout", 0))
    seconds_value = item.get(
        "est_seconds",
        item.get("estimated_seconds", item.get("duration_seconds", 0)),
    )

    try:
        payout = Decimal(str(payout_value))
        seconds_decimal = Decimal(str(seconds_value))
    except (InvalidOperation, TypeError, ValueError):
        return None

    if not payout.is_finite() or payout < 0:
        return None

    if not seconds_decimal.is_finite():
        return None

    if seconds_decimal <= 0 or seconds_decimal != seconds_decimal.to_integral_value():
        return None

    seconds = int(seconds_decimal)
    if seconds > 2_147_483_647:
        return None

    external_id = item.get("task_id") or item.get("id")
    canonical = json.dumps(
        item,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )

    if external_id is None:
        identity = canonical
    else:
        identity = str(external_id)

    digest = hashlib.sha256(
        f"{platform_name}:{identity}".encode("utf-8")
    ).hexdigest()

    task_id = f"se-{digest[:48]}"
    category = str(item.get("category", "general"))[:120]

    return {
        "task_id": task_id,
        "category": category,
        "payload": canonical,
        "payout_usd": payout,
        "est_seconds": seconds,
    }


def scan_one_target(target: dict) -> dict:
    if target["extraction_method"] != "httpx":
        return {
            "platform": target["platform_name"],
            "status": "skipped",
            "reason": "This API scan supports httpx targets only; use worker.py for other methods.",
            "inserted": 0,
        }

    endpoint = validate_endpoint_url(
        target["api_endpoint"],
        target["extraction_method"],
    )

    try:
        with httpx.Client(
            timeout=SCAN_TIMEOUT_SECONDS,
            follow_redirects=False,
            headers={"Accept": "application/json", "User-Agent": "SolveEaseTaskEngine/2.0"},
        ) as client:
            response = client.get(endpoint)
            response.raise_for_status()
            data = response.json()

        items = extract_task_list(data)
        inserted = 0
        invalid = 0

        with closing(connect_database()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    for item in items:
                        normalized = normalize_task(item, target["platform_name"])

                        if normalized is None:
                            invalid += 1
                            continue

                        cur.execute(
                            """
                            INSERT INTO tasks (
                                task_id,
                                source_target_id,
                                source_platform,
                                category,
                                payload,
                                payout_usd,
                                est_seconds
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (task_id) DO NOTHING
                            RETURNING id
                            """,
                            (
                                normalized["task_id"],
                                target["id"],
                                target["platform_name"],
                                normalized["category"],
                                normalized["payload"],
                                normalized["payout_usd"],
                                normalized["est_seconds"],
                            ),
                        )

                        row = cur.fetchone()
                        if row:
                            inserted += 1
                            cur.execute(
                                """
                                INSERT INTO task_events
                                    (task_id, event_type, details)
                                VALUES (%s, %s, %s)
                                """,
                                (
                                    row["id"],
                                    "discovered",
                                    json.dumps({
                                        "source": target["platform_name"],
                                        "category": normalized["category"],
                                    }),
                                ),
                            )

                    cur.execute(
                        """
                        UPDATE targets
                        SET last_checked = NOW(),
                            consecutive_failures = 0,
                            consecutive_empty_polls =
                                CASE WHEN %s = 0
                                     THEN consecutive_empty_polls + 1
                                     ELSE 0 END
                        WHERE id = %s
                        """,
                        (len(items), target["id"]),
                    )

        return {
            "platform": target["platform_name"],
            "status": "ok",
            "received": len(items),
            "inserted": inserted,
            "invalid": invalid,
        }

    except Exception as exc:
        database_error(f"Scan for {target['platform_name']}", exc)

        try:
            with closing(connect_database()) as conn:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE targets
                            SET last_checked = NOW(),
                                consecutive_failures =
                                    consecutive_failures + 1
                            WHERE id = %s
                            """,
                            (target["id"],),
                        )
        except Exception as update_exc:
            database_error("Target failure counter update", update_exc)

        return {
            "platform": target["platform_name"],
            "status": "error",
            "reason": "Feed scan failed. Check server logs and target configuration.",
            "inserted": 0,
        }


def execute_engine_scan_cycle():
    require_database()

    with closing(connect_database()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, platform_name, api_endpoint, extraction_method, tier
                FROM targets
                WHERE is_active = TRUE
                ORDER BY tier ASC, id ASC
                """
            )
            targets = cur.fetchall()

    results = []
    for target in targets:
        results.append(scan_one_target(target))

    inserted_total = sum(item.get("inserted", 0) for item in results)

    return {
        "targets_checked": len(results),
        "tasks_inserted": inserted_total,
        "results": results,
    }


# ============================================================
# OPTIONAL SAFE SEEDING
# ============================================================

def seed_targets_if_enabled():
    if not AUTO_SEED_TARGETS:
        logger.info("Automatic target seeding disabled.")
        return

    path = Path("targets_300.json")
    if not path.exists():
        logger.info("targets_300.json not found; seeding skipped.")
        return

    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, dict):
            data = data.get("targets", [])

        if not isinstance(data, list):
            raise ValueError("Seed JSON must contain a list of targets")

        valid_targets = []
        for item in data:
            if not isinstance(item, dict):
                continue

            model = TargetRegistration(
                platform_name=item.get("platform_name", ""),
                api_endpoint=item.get("api_endpoint", ""),
                extraction_method=item.get("extraction_method", "httpx"),
                tier=item.get("tier", 2),
                is_active=False,
            )
            valid_targets.append(model)

        with closing(connect_database()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    for target in valid_targets:
                        cur.execute(
                            """
                            INSERT INTO targets (
                                platform_name,
                                api_endpoint,
                                extraction_method,
                                tier,
                                is_active
                            )
                            VALUES (%s, %s, %s, %s, FALSE)
                            ON CONFLICT (platform_name) DO NOTHING
                            """,
                            (
                                target.platform_name,
                                target.api_endpoint,
                                target.extraction_method,
                                target.tier,
                            ),
                        )

        logger.info("Target seeding completed; valid entries: %s", len(valid_targets))

    except Exception as exc:
        logger.error(
            "Target seeding skipped: %s (%s)",
            type(exc).__name__,
            str(exc)[:200],
        )


@app.on_event("startup")
def startup_event():
    logger.info("Starting %s version %s", APP_NAME, APP_VERSION)
    seed_targets_if_enabled()

    if ENABLE_API_BACKGROUND_SCANNER:
        logger.warning(
            "ENABLE_API_BACKGROUND_SCANNER=true. Ensure worker.py is not "
            "also polling the same targets."
        )
    else:
        logger.info("API background scanning disabled; worker.py owns continuous polling.")


# ============================================================
# PUBLIC STATUS
# ============================================================

@app.get("/")
def root():
    return {
        "service": APP_NAME,
        "version": APP_VERSION,
        "status": "online",
        "database": "Neon PostgreSQL",
        "continuous_scanner": "worker.py",
        "task_consumer": "consumer.py",
        "note": "Online status does not prove that feeds or task providers are working.",
    }


@app.get("/api/health")
def health():
    try:
        with closing(connect_database()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        to_regclass('public.targets') AS targets_table,
                        to_regclass('public.tasks') AS tasks_table,
                        to_regclass('public.task_events') AS events_table
                    """
                )
                tables = cur.fetchone()

        missing = [
            name for name, value in [
                ("targets", tables["targets_table"]),
                ("tasks", tables["tasks_table"]),
                ("task_events", tables["events_table"]),
            ]
            if value is None
        ]

        if missing:
            return {
                "status": "degraded",
                "database": "connected",
                "missing_tables": missing,
            }

        return {
            "status": "healthy",
            "database": "connected",
            "tables": "present",
        }

    except Exception as exc:
        database_error("Health check", exc)
        raise HTTPException(
            status_code=503,
            detail="Database unavailable or schema not ready.",
        ) from None


# ============================================================
# AUTHENTICATED API ENDPOINTS
# ============================================================

@app.get("/api/summary")
def summary(_: bool = Depends(verify_api_key)):
    require_database()

    try:
        with closing(connect_database()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_targets,
                        COUNT(*) FILTER (WHERE is_active) AS active_targets
                    FROM targets
                    """
                )
                target_counts = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total_tasks,
                        COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                        COUNT(*) FILTER (WHERE status = 'processing') AS processing,
                        COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed
                    FROM tasks
                    """
                )
                task_counts = cur.fetchone()

        return {
            "targets": target_counts,
            "tasks": task_counts,
        }

    except Exception as exc:
        database_error("Summary endpoint", exc)
        raise HTTPException(status_code=503, detail="Unable to retrieve summary.") from None


@app.get("/api/queue/top")
def queue_top(
    limit: int = 20,
    _: bool = Depends(verify_api_key),
):
    require_database()
    limit = max(1, min(limit, 100))

    try:
        with closing(connect_database()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, task_id, source_platform, category,
                        payout_usd, est_seconds, roc_score,
                        status, created_at
                    FROM tasks
                    WHERE status = 'pending'
                    ORDER BY roc_score DESC, created_at ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

        return {"count": len(rows), "tasks": rows}

    except Exception as exc:
        database_error("Queue endpoint", exc)
        raise HTTPException(status_code=503, detail="Unable to retrieve task queue.") from None


@app.get("/api/targets")
def list_targets(_: bool = Depends(verify_api_key)):
    require_database()

    try:
        with closing(connect_database()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, platform_name, api_endpoint, extraction_method,
                        tier, consecutive_empty_polls, consecutive_failures,
                        is_active, last_checked, cooldown_until, created_at
                    FROM targets
                    ORDER BY tier ASC, platform_name ASC
                    """
                )
                rows = cur.fetchall()

        return {"count": len(rows), "targets": rows}

    except Exception as exc:
        database_error("Target listing", exc)
        raise HTTPException(status_code=503, detail="Unable to retrieve targets.") from None


def register_one_target(target: TargetRegistration):
    with closing(connect_database()) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO targets (
                        platform_name,
                        api_endpoint,
                        extraction_method,
                        tier,
                        is_active
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (platform_name)
                    DO UPDATE SET
                        api_endpoint = EXCLUDED.api_endpoint,
                        extraction_method = EXCLUDED.extraction_method,
                        tier = EXCLUDED.tier
                    RETURNING id, platform_name, extraction_method, tier, is_active
                    """,
                    (
                        target.platform_name,
                        target.api_endpoint,
                        target.extraction_method,
                        target.tier,
                        target.is_active,
                    ),
                )
                return cur.fetchone()


@app.post("/api/targets/register")
def register_target(
    target: TargetRegistration,
    _: bool = Depends(verify_api_key),
):
    try:
        row = register_one_target(target)
        return {"status": "ok", "target": row}
    except HTTPException:
        raise
    except Exception as exc:
        database_error("Target registration", exc)
        raise HTTPException(status_code=503, detail="Target registration failed.") from None


@app.post("/api/targets/bulk-register")
def bulk_register_targets(
    targets: list[TargetRegistration] = Body(...),
    _: bool = Depends(verify_api_key),
):
    if not targets:
        raise HTTPException(status_code=400, detail="Provide at least one target.")

    if len(targets) > 300:
        raise HTTPException(status_code=400, detail="Maximum 300 targets per request.")

    registered = []
    errors = []

    for target in targets:
        try:
            registered.append(register_one_target(target))
        except Exception as exc:
            database_error("Bulk target registration", exc)
            errors.append({
                "platform_name": target.platform_name,
                "error": "Registration failed",
            })

    return {
        "status": "partial" if errors else "ok",
        "registered_count": len(registered),
        "error_count": len(errors),
        "registered": registered,
        "errors": errors,
    }


@app.patch("/api/targets/{platform_name}/status")
def update_target_status(
    platform_name: str,
    update: TargetStatusUpdate,
    _: bool = Depends(verify_api_key),
):
    try:
        with closing(connect_database()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE targets
                        SET is_active = %s
                        WHERE platform_name = %s
                        RETURNING id, platform_name, is_active
                        """,
                        (update.is_active, platform_name),
                    )
                    row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Target not found.")

        return {"status": "ok", "target": row}

    except HTTPException:
        raise
    except Exception as exc:
        database_error("Target status update", exc)
        raise HTTPException(status_code=503, detail="Target status update failed.") from None


@app.post("/api/engine/trigger")
def trigger_engine(_: bool = Depends(verify_api_key)):
    try:
        result = execute_engine_scan_cycle()
        send_telegram_alert(
            "SolveEase manual scan finished.\n"
            f"Targets checked: {result['targets_checked']}\n"
            f"New tasks inserted: {result['tasks_inserted']}"
        )
        return {"status": "finished", **result}

    except HTTPException:
        raise
    except Exception as exc:
        database_error("Manual engine trigger", exc)
        raise HTTPException(status_code=503, detail="Manual scan failed.") from None
