
#!/usr/bin/env python3
"""
SolveEase 300-Target Task Engine API
Neon PostgreSQL + background scanner + Telegram monitoring.

Render start command:
    uvicorn main:app --host 0.0.0.0 --port $PORT

Required:
    DATABASE_URL
    ENGINE_API_KEY

Optional:
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID
    SCAN_INTERVAL_SECONDS=900
    REQUEST_TIMEOUT_SECONDS=15

Task execution remains in consumer.py. This API never marks a task
completed merely because it was discovered or placed in a queue.
"""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.rows import dict_row

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator


# ==================================================
# CONFIGURATION
# ==================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("solveease.engine")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
API_KEY = os.getenv("ENGINE_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SCAN_INTERVAL_SECONDS = max(
    60, int(os.getenv("SCAN_INTERVAL_SECONDS", "900"))
)
REQUEST_TIMEOUT_SECONDS = max(
    3, min(60, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15")))
)

ALLOWED_METHODS = {"httpx", "playwright", "websocket"}
MAX_RESPONSE_BYTES = 2_000_000
MAX_BULK_TARGETS = 100

security = HTTPBearer(auto_error=False)


# ==================================================
# DATABASE
# ==================================================

def connect_database():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")

    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=10,
        sslmode="require",
        row_factory=dict_row,
    )


def query_db(sql: str, params=()):
    try:
        with connect_database() as conn:
            return [
                dict(row)
                for row in conn.execute(sql, params).fetchall()
            ]
    except (psycopg.Error, RuntimeError) as exc:
        logger.error("Database query failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Database operation failed.",
        ) from exc


def record_event(conn, task_id: int, event_type: str, details: str = ""):
    conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, details)
        VALUES (%s, %s, %s)
        """,
        (task_id, event_type, details[:2000]),
    )


# ==================================================
# TELEGRAM
# ==================================================

def send_telegram_alert(message: str):
    """Send a best-effort Telegram alert without exposing bot credentials."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram not configured; alert: %s", message[:500])
        return False

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    try:
        with httpx.Client(timeout=10) as client:
            response = client.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message[:4000],
                },
            )
            response.raise_for_status()

            body = response.json()
            if not body.get("ok"):
                logger.warning("Telegram rejected an alert.")
                return False

            return True

    except Exception as exc:
        logger.warning("Telegram delivery failed: %s", type(exc).__name__)
        return False


# ==================================================
# AUTHENTICATION
# ==================================================

def verify_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    if not API_KEY:
        raise HTTPException(
            status_code=503,
            detail="ENGINE_API_KEY is not configured.",
        )

    if credentials is None or not hmac.compare_digest(
        credentials.credentials, API_KEY
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return True


# ==================================================
# TARGET VALIDATION
# ==================================================

def validate_target_values(
    platform_name: str,
    endpoint: str,
    method: str,
    tier: int,
):
    name = platform_name.strip()
    endpoint = endpoint.strip()
    method = method.strip().lower()

    if not 2 <= len(name) <= 100:
        raise ValueError("Platform name must be 2–100 characters.")

    if method not in ALLOWED_METHODS:
        raise ValueError("Unsupported extraction method.")

    if tier not in (1, 2):
        raise ValueError("Tier must be 1 or 2.")

    try:
        parsed = urlparse(endpoint)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("Malformed endpoint URL.") from exc

    if parsed.scheme not in {"https", "wss"}:
        raise ValueError("Target must use HTTPS or WSS.")

    expected_scheme = "wss" if method == "websocket" else "https"
    if parsed.scheme != expected_scheme:
        raise ValueError(f"{method} requires {expected_scheme}://")

    if (
        not hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise ValueError(
            "Endpoint must have a hostname, no embedded credentials, "
            "and no fragment."
        )

    if hostname.lower() in {"localhost", "localhost.localdomain"}:
        raise ValueError("Localhost targets are not permitted.")

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None

    if address is not None and not address.is_global:
        raise ValueError("Non-public IP addresses are not permitted.")

    return name, endpoint, method, tier


class TargetRegistration(BaseModel):
    platform_name: str = Field(min_length=2, max_length=100)
    api_endpoint: str = Field(min_length=8, max_length=2048)
    extraction_method: str
    tier: int = Field(default=1, ge=1, le=2)

    @field_validator("platform_name")
    @classmethod
    def clean_name(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Platform name cannot be empty.")
        return value

    @field_validator("extraction_method")
    @classmethod
    def clean_method(cls, value):
        value = value.strip().lower()
        if value not in ALLOWED_METHODS:
            raise ValueError("Method must be httpx, playwright, or websocket.")
        return value

    @field_validator("api_endpoint")
    @classmethod
    def clean_endpoint(cls, value):
        value = value.strip()
        try:
            parsed = urlparse(value)
            hostname = parsed.hostname
            _ = parsed.port
        except ValueError as exc:
            raise ValueError("Malformed endpoint URL.") from exc

        if parsed.scheme not in {"https", "wss"}:
            raise ValueError("Endpoint must use HTTPS or WSS.")
        if not hostname or parsed.username or parsed.password:
            raise ValueError("Endpoint hostname is invalid.")
        if parsed.fragment:
            raise ValueError("URL fragments are not permitted.")

        if hostname.lower() in {"localhost", "localhost.localdomain"}:
            raise ValueError("Localhost targets are not permitted.")

        try:
            address = ipaddress.ip_address(hostname)
        except ValueError:
            address = None

        if address is not None and not address.is_global:
            raise ValueError("Non-public IP addresses are not permitted.")

        return value


class TargetStatusUpdate(BaseModel):
    is_active: bool


# ==================================================
# FEED PARSING
# ==================================================

def parse_feed_payload(data):
    """
    Accepted feed formats:
      - a JSON list of task objects
      - {"tasks": [...]}
      - {"data": [...]}

    Each task needs a stable task_id or id, payout_usd and est_seconds.
    """
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("tasks", data.get("data", []))
    else:
        raise ValueError("Feed root must be a JSON list or object.")

    if not isinstance(items, list):
        raise ValueError("'tasks' or 'data' must be a list.")

    valid = []

    for item in items[:1000]:
        if not isinstance(item, dict):
            continue

        source_id = item.get("task_id", item.get("id"))
        if source_id is None or not str(source_id).strip():
            # Never invent an unstable ID for a real opportunity.
            continue

        try:
            payout = float(item.get("payout_usd"))
            seconds = int(item.get("est_seconds"))
        except (TypeError, ValueError, OverflowError):
            continue

        if not math.isfinite(payout) or payout < 0 or seconds <= 0:
            continue

        category = str(item.get("category", "General"))[:200]
        task_id = str(source_id).strip()[:300]
        payload = json.dumps(item, ensure_ascii=False, separators=(",", ":"))

        valid.append({
            "source_id": task_id,
            "category": category,
            "payout_usd": payout,
            "est_seconds": seconds,
            "payload": payload,
        })

    return valid


def fetch_target_items(target):
    """
    Fetch only feeds using the documented JSON format.
    Unsupported extraction methods are reported, not silently treated
    as successful scans.
    """
    method = target["extraction_method"]
    endpoint = target["api_endpoint"]

    if method != "httpx":
        raise ValueError(
            f"{method} extraction is not implemented in this scanner. "
            "Use a compatible JSON HTTPS feed or implement a dedicated adapter."
        )

    with httpx.Client(
        timeout=REQUEST_TIMEOUT_SECONDS,
        follow_redirects=False,
        headers={"Accept": "application/json"},
    ) as client:
        with client.stream("GET", endpoint) as response:
            response.raise_for_status()

            content_type = response.headers.get("content-type", "").lower()
            if "json" not in content_type:
                raise ValueError("Feed did not return a JSON content type.")

            chunks = []
            total_bytes = 0

            for chunk in response.iter_bytes():
                total_bytes += len(chunk)
                if total_bytes > MAX_RESPONSE_BYTES:
                    raise ValueError("Feed response exceeded the size limit.")
                chunks.append(chunk)

            data = json.loads(b"".join(chunks).decode("utf-8"))

    return parse_feed_payload(data)


# ==================================================
# ENGINE SCAN
# ==================================================

def execute_engine_scan_cycle() -> dict:
    """
    Scan active targets and insert newly discovered tasks.

    The generated roc_score column is computed by PostgreSQL.
    Task discovery is not task execution or confirmed revenue.
    """
    scanned = 0
    discovered = 0
    errors = []

    with connect_database() as conn:
        targets = conn.execute(
            """
            SELECT id, platform_name, api_endpoint, extraction_method, tier
            FROM targets
            WHERE is_active = TRUE
              AND (cooldown_until IS NULL OR cooldown_until <= NOW())
            ORDER BY tier, consecutive_failures, platform_name
            """
        ).fetchall()

        for target in targets:
            scanned += 1
            name = target["platform_name"]

            try:
                items = fetch_target_items(target)
                inserted_count = 0

                # Each target has its own transaction boundary. A failed
                # feed does not undo previously committed target results.
                with conn.transaction():
                    for item in items:
                        # Stable, namespaced identifier across restarts.
                        stable_id = hashlib.sha256(
                            f"{name}\0{item['source_id']}".encode("utf-8")
                        ).hexdigest()

                        task_id = f"{name}:{stable_id}"

                        row = conn.execute(
                            """
                            INSERT INTO tasks (
                                task_id,
                                source_target_id,
                                source_platform,
                                category,
                                payload,
                                payout_usd,
                                est_seconds,
                                status,
                                available_at
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s,
                                'pending', NOW()
                            )
                            ON CONFLICT (task_id) DO NOTHING
                            RETURNING id
                            """,
                            (
                                task_id,
                                target["id"],
                                name,
                                item["category"],
                                item["payload"],
                                item["payout_usd"],
                                item["est_seconds"],
                            ),
                        ).fetchone()

                        if row:
                            inserted_count += 1
                            record_event(
                                conn,
                                row["id"],
                                "discovered",
                                f"Discovered from {name}",
                            )

                    conn.execute(
                        """
                        UPDATE targets
                        SET last_checked = NOW(),
                            consecutive_failures = 0,
                            consecutive_empty_polls =
                                CASE WHEN %s = 0
                                     THEN consecutive_empty_polls + 1
                                     ELSE 0 END,
                            cooldown_until = NULL
                        WHERE id = %s
                        """,
                        (inserted_count, target["id"]),
                    )

                discovered += inserted_count

            except Exception as exc:
                error_type = type(exc).__name__
                logger.warning(
                    "Scan failed for target %s (%s)",
                    name,
                    error_type,
                )

                errors.append({
                    "platform_name": name,
                    "error_type": error_type,
                })

                # Keep the error detail out of public API responses.
                try:
                    with conn.transaction():
                        conn.execute(
                            """
                            UPDATE targets
                            SET consecutive_failures = consecutive_failures + 1,
                                last_checked = NOW(),
                                cooldown_until =
                                    CASE
                                        WHEN consecutive_failures + 1 >= 3
                                        THEN NOW() + INTERVAL '5 minutes'
                                        ELSE cooldown_until
                                    END
                            WHERE id = %s
                            """,
                            (target["id"],),
                        )
                except psycopg.Error:
                    logger.exception(
                        "Could not record failure for target %s", name
                    )

    report = (
        "SolveEase Scan Report\n"
        f"Active targets scanned: {scanned}\n"
        f"New tasks discovered: {discovered}\n"
        f"Target errors: {len(errors)}\n"
        "Note: discovered tasks are not confirmed earnings."
    )

    send_telegram_alert(report)

    return {
        "status": "completed" if not errors else "completed_with_errors",
        "scanned_targets": scanned,
        "tasks_discovered": discovered,
        "errors": errors,
    }


# ==================================================
# LIFESPAN / BACKGROUND SCANNER
# ==================================================

async def background_scanning_loop():
    await asyncio.sleep(15)

    while True:
        try:
            logger.info("Starting scheduled feed scan.")
            result = await asyncio.to_thread(execute_engine_scan_cycle)
            logger.info(
                "Scan complete: scanned=%s discovered=%s errors=%s",
                result["scanned_targets"],
                result["tasks_discovered"],
                len(result["errors"]),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Background scan failed.")

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting SolveEase API.")

    app.state.scanner_task = asyncio.create_task(
        background_scanning_loop()
    )

    try:
        yield
    finally:
        app.state.scanner_task.cancel()
        try:
            await app.state.scanner_task
        except asyncio.CancelledError:
            pass
        logger.info("SolveEase API shutdown complete.")


app = FastAPI(
    title="SolveEase 300-Target Task Engine API",
    version="1.9.0",
    lifespan=lifespan,
    description="Neon-backed public opportunity discovery API.",
)


# ==================================================
# ROOT AND HEALTH
# ==================================================

@app.get("/")
def root():
    return {
        "service": "SolveEase 300-Target Task Engine API",
        "version": "1.9.0",
        "status": "running",
        "database": "Neon PostgreSQL",
        "health_endpoint": "/api/health",
        "documentation": "/docs",
        "openapi": "/openapi.json",
    }


@app.get("/api/health")
def health():
    try:
        with connect_database() as conn:
            conn.execute("SELECT 1").fetchone()

            tables = conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_type = 'BASE TABLE'
                  AND table_name IN (
                      'targets', 'tasks', 'task_events'
                  )
                """
            ).fetchall()

            found = {row["table_name"] for row in tables}
            missing = sorted(
                {"targets", "tasks", "task_events"} - found
            )

            if missing:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "Required database tables are missing.",
                        "missing_tables": missing,
                    },
                )

        return {
            "status": "ok",
            "database": "connected",
            "schema": "verified",
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Health check failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check DATABASE_URL and Neon.",
        ) from exc


# ==================================================
# SUMMARY AND QUEUE
# ==================================================

@app.get(
    "/api/summary",
    dependencies=[Depends(verify_api_key)],
)
def get_summary():
    rows = query_db(
        """
        SELECT
            (SELECT COUNT(*) FROM targets) AS targets,
            (SELECT COUNT(*) FROM targets
             WHERE is_active = TRUE) AS active_targets,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'pending') AS pending,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'processing') AS processing,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'completed') AS completed,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'failed') AS failed
        """
    )
    return rows[0]


@app.get(
    "/api/queue/top",
    dependencies=[Depends(verify_api_key)],
)
def get_top_tasks(limit: int = Query(default=10, ge=1, le=100)):
    tasks = query_db(
        """
        SELECT
            task_id, source_platform, category, payout_usd,
            est_seconds, roc_score, status, created_at
        FROM tasks
        WHERE status = 'pending'
          AND available_at <= NOW()
        ORDER BY roc_score DESC, created_at ASC, id ASC
        LIMIT %s
        """,
        (limit,),
    )
    return {"count": len(tasks), "top_queue": tasks}


@app.get(
    "/api/targets",
    dependencies=[Depends(verify_api_key)],
)
def get_targets():
    targets = query_db(
        """
        SELECT
            id, platform_name, api_endpoint, extraction_method, tier,
            is_active, consecutive_empty_polls, consecutive_failures,
            last_checked, cooldown_until
        FROM targets
        ORDER BY tier, platform_name
        """
    )
    return {"count": len(targets), "targets": targets}


# ==================================================
# REGISTER ONE OR MANY TARGETS
# ==================================================

@app.post(
    "/api/targets/register",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
)
def register_target(target: TargetRegistration):
    try:
        name, endpoint, method, tier = validate_target_values(
            target.platform_name,
            target.api_endpoint,
            target.extraction_method,
            target.tier,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        with connect_database() as conn:
            row = conn.execute(
                """
                INSERT INTO targets (
                    platform_name, api_endpoint, extraction_method,
                    tier, is_active
                )
                VALUES (%s, %s, %s, %s, FALSE)
                ON CONFLICT (platform_name) DO NOTHING
                RETURNING platform_name
                """,
                (name, endpoint, method, tier),
            ).fetchone()

            if not row:
                raise HTTPException(
                    status_code=409,
                    detail="Target with this platform name already exists.",
                )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Target registration failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Target registration failed.",
        ) from exc

    return {
        "status": "registered",
        "platform_name": name,
        "tier": tier,
        "is_active": False,
        "message": "Registered inactive. Verify the source before activation.",
    }


@app.post(
    "/api/targets/bulk-register",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
)
def bulk_register_targets(
    targets: list[TargetRegistration] = Field(max_length=MAX_BULK_TARGETS),
):
    registered = 0
    skipped = 0

    try:
        with connect_database() as conn:
            for target in targets:
                name, endpoint, method, tier = validate_target_values(
                    target.platform_name,
                    target.api_endpoint,
                    target.extraction_method,
                    target.tier,
                )

                row = conn.execute(
                    """
                    INSERT INTO targets (
                        platform_name, api_endpoint,
                        extraction_method, tier, is_active
                    )
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (platform_name) DO NOTHING
                    RETURNING platform_name
                    """,
                    (name, endpoint, method, tier),
                ).fetchone()

                if row:
                    registered += 1
                else:
                    skipped += 1

    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Bulk registration failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Bulk registration failed.",
        ) from exc

    return {
        "status": "completed",
        "registered": registered,
        "skipped": skipped,
        "total": len(targets),
    }


# ==================================================
# TARGET ACTIVATION
# ==================================================

@app.patch(
    "/api/targets/{platform_name}/status",
    dependencies=[Depends(verify_api_key)],
)
def set_target_status(
    platform_name: str,
    status_update: TargetStatusUpdate,
):
    try:
        with connect_database() as conn:
            row = conn.execute(
                """
                UPDATE targets
                SET is_active = %s
                WHERE platform_name = %s
                RETURNING platform_name, is_active
                """,
                (status_update.is_active, platform_name),
            ).fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail="Target not found.",
                )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Target status update failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Target status update failed.",
        ) from exc

    return {
        "status": "updated",
        "platform_name": row["platform_name"],
        "is_active": row["is_active"],
    }


# ==================================================
# MANUAL SCAN
# ==================================================

@app.post(
    "/api/engine/trigger",
    dependencies=[Depends(verify_api_key)],
)
async def trigger_engine_poll():
    try:
        result = await asyncio.to_thread(execute_engine_scan_cycle)
        return {
            "status": "completed",
            **result,
            "note": "Discovery only; no task completion or payout is implied.",
        }

    except Exception as exc:
        logger.error("Manual scan failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Engine scan failed. Check service logs.",
        ) from exc
