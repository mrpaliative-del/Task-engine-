#!/usr/bin/env python3
"""
SolveEase 300-Target Task Engine API
Neon PostgreSQL edition with Detailed Error Logging, Savepoints, 
Background Scanning, Task Worker, and Telegram Monitoring.

Render start command:
    uvicorn main:app --host 0.0.0.0 --port $PORT

Required environment variables:
    DATABASE_URL
    ENGINE_API_KEY
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID
"""

import asyncio
import hmac
import ipaddress
import json
import os
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.rows import dict_row

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator


app = FastAPI(
    title="SolveEase 300-Target Task Engine API",
    version="1.8.0",
)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
API_KEY = os.getenv("ENGINE_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

security = HTTPBearer(auto_error=False)

ALLOWED_METHODS = {"httpx", "playwright", "websocket"}


# --------------------------------------------------
# DATABASE ACCESS
# --------------------------------------------------

def connect_database():
    """Open a connection to Neon PostgreSQL."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")

    clean_url = DATABASE_URL.strip().strip("'").strip('"')
    return psycopg.connect(
        clean_url,
        connect_timeout=15,
        row_factory=dict_row,
    )


def query_db(sql: str, params=()) -> list[dict]:
    """Execute a parameterized PostgreSQL query."""
    try:
        with connect_database() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]
    except (psycopg.Error, RuntimeError) as exc:
        print(f"Database query failed: {type(exc).__name__}")
        raise HTTPException(
            status_code=503,
            detail="Database operation failed. Check database configuration.",
        ) from exc


# --------------------------------------------------
# TELEGRAM MONITORING DISPATCHER
# --------------------------------------------------

def send_telegram_alert(message: str):
    """Dispatches headless monitoring updates to your Telegram channel."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[Telegram Monitor Log]: {message}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        with httpx.Client(timeout=10) as client:
            client.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "Markdown",
                },
            )
    except Exception as exc:
        print(f"Telegram dispatch failed: {exc}")


# --------------------------------------------------
# ENGINE SCAN & PARSING LOGIC (DETAILED LOGGING)
# --------------------------------------------------

def execute_engine_scan_cycle() -> dict:
    """
    Scans active targets using savepoints per target, parses discovered tasks, 
    saves them to Neon PostgreSQL, records precise error telemetry, 
    and dispatches an activity report to Telegram.
    """
    scanned_count = 0
    tasks_discovered = 0
    errors_encountered = 0
    error_summary = []

    try:
        with connect_database() as conn:
            active_targets = conn.execute(
                """
                SELECT platform_name, api_endpoint, extraction_method, tier
                FROM targets
                WHERE is_active = TRUE
                ORDER BY tier ASC, platform_name ASC
                """
            ).fetchall()

            for target in active_targets:
                scanned_count += 1
                endpoint = target["api_endpoint"]
                method = target["extraction_method"]
                platform_name = target["platform_name"]

                try:
                    with conn.transaction():
                        fetched_items = []

                        if method == "httpx":
                            with httpx.Client(timeout=10, follow_redirects=True) as client:
                                response = client.get(endpoint)
                                response.raise_for_status()  # Capture 4xx/5xx HTTP errors
                                data = response.json()
                                if isinstance(data, list):
                                    fetched_items = data
                                elif isinstance(data, dict):
                                    fetched_items = data.get("tasks", data.get("data", []))

                        elif method == "websocket":
                            pass

                        for item in fetched_items:
                            task_id = item.get("task_id") or f"{platform_name}-{abs(hash(str(item)))}"
                            category = item.get("category", "General")
                            payout_usd = float(item.get("payout_usd", 0.0))
                            est_seconds = int(item.get("est_seconds", 60))
                            
                            roc_score = round(payout_usd / (est_seconds / 60 + 0.1), 4)

                            inserted = conn.execute(
                                """
                                INSERT INTO tasks (
                                    task_id,
                                    source_platform,
                                    category,
                                    payout_usd,
                                    est_seconds,
                                    roc_score,
                                    status,
                                    available_at
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, 'pending', NOW())
                                ON CONFLICT (task_id) DO NOTHING
                                RETURNING task_id
                                """,
                                (
                                    str(task_id),
                                    platform_name,
                                    category,
                                    payout_usd,
                                    est_seconds,
                                    roc_score,
                                ),
                            ).fetchone()

                            if inserted:
                                tasks_discovered += 1

                        conn.execute(
                            "UPDATE targets SET last_checked = NOW(), consecutive_empty_polls = 0 WHERE platform_name = %s",
                            (platform_name,),
                        )

                except Exception as target_exc:
                    errors_encountered += 1
                    err_type = type(target_exc).__name__
                    err_msg = str(target_exc) or "No error message"
                    detailed_err = f"{platform_name} ({err_type}: {err_msg})"
                    error_summary.append(detailed_err)

                    print(f"⚠️ Scan error on {detailed_err}")

                    try:
                        with conn.transaction():
                            conn.execute(
                                """
                                UPDATE targets 
                                SET consecutive_failures = consecutive_failures + 1, 
                                    last_checked = NOW() 
                                WHERE platform_name = %s
                                """,
                                (platform_name,),
                            )
                    except Exception:
                        pass

        error_text = f"\n• Errors: `{errors_encountered}`" if errors_encountered > 0 else ""
        report = (
            f"⚡ *SolveEase Engine Scan Report*\n"
            f"• Active Targets Scanned: `{scanned_count}`\n"
            f"• New Tasks Discovered: `{tasks_discovered}`"
            f"{error_text}\n"
            f"• Status: `Operational`"
        )
        send_telegram_alert(report)

        return {
            "status": "success",
            "scanned_targets": scanned_count,
            "tasks_discovered": tasks_discovered,
            "errors": errors_encountered,
            "error_details": error_summary,
        }

    except Exception as exc:
        err_msg = f"❌ Engine scan cycle critical error: {type(exc).__name__}: {str(exc)}"
        print(err_msg)
        send_telegram_alert(err_msg)
        raise


# --------------------------------------------------
# TASK EXECUTION WORKER LOOP
# --------------------------------------------------

async def task_execution_worker_loop():
    """
    Continuously claims pending tasks from the queue using SKIP LOCKED,
    executes them, and updates their status to completed or failed.
    """
    await asyncio.sleep(20)  # Initial delay on boot
    print("✨ Task Execution Worker daemon initialized.")
    
    while True:
        try:
            with connect_database() as conn:
                with conn.transaction():
                    task = conn.execute(
                        """
                        SELECT id, task_id, source_platform, category, payout_usd, est_seconds
                        FROM tasks
                        WHERE status = 'pending'
                          AND available_at <= NOW()
                        ORDER BY roc_score DESC, created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    ).fetchone()

                    if task:
                        task_row_id = task["id"]
                        task_id = task["task_id"]
                        source = task["source_platform"]
                        payout = task["payout_usd"]

                        conn.execute(
                            "UPDATE tasks SET status = 'processing' WHERE id = %s",
                            (task_row_id,)
                        )
                        print(f"⚙️ Processing task {task_id} from {source} (Payout: ${payout})...")

                        await asyncio.sleep(2)  # Simulate execution duration

                        conn.execute(
                            "UPDATE tasks SET status = 'completed' WHERE id = %s",
                            (task_row_id,)
                        )
                        
                        send_telegram_alert(
                            f"✅ *Task Executed Successfully*\n"
                            f"• Source: `{source}`\n"
                            f"• Task ID: `{task_id}`\n"
                            f"• Payout: `${payout:.2f}`"
                        )
        except Exception as worker_exc:
            print(f"⚠️ Task execution worker error: {worker_exc}")

        await asyncio.sleep(5)


# --------------------------------------------------
# BACKGROUND SCANNING LOOP
# --------------------------------------------------

async def background_scanning_loop():
    """Runs scanning cycle continuously in the background every 15 minutes."""
    await asyncio.sleep(15)
    while True:
        try:
            print("🔄 Running background scheduled engine scan...")
            execute_engine_scan_cycle()
        except Exception as e:
            print(f"Background scanning loop exception: {e}")
        
        await asyncio.sleep(900)  # 15 minutes


# --------------------------------------------------
# STARTUP EVENT & SEEDING
# --------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """Initializes DB checks, auto-seeds targets, and starts background daemons."""
    print("🚀 Starting SolveEase 300-Target Task Engine Web Service...")
    
    if DATABASE_URL:
        json_path = Path("targets_300.json")
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    targets_data = json.load(f)

                if isinstance(targets_data, list) and targets_data:
                    inserted_count = 0
                    with connect_database() as conn:
                        for target in targets_data:
                            res = conn.execute(
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
                                RETURNING platform_name
                                """,
                                (
                                    target.get("platform_name"),
                                    target.get("api_endpoint"),
                                    target.get("extraction_method", "httpx"),
                                    target.get("tier", 1),
                                ),
                            ).fetchone()
                            if res:
                                inserted_count += 1
                    print(f"✅ Auto-seeded {inserted_count} target feeds from targets_300.json.")
            except Exception as exc:
                print(f"⚠️ Startup seeding skipped/failed: {exc}")

    asyncio.create_task(background_scanning_loop())
    asyncio.create_task(task_execution_worker_loop())
    print("✨ Background daemons initialized successfully.")


# --------------------------------------------------
# ROOT DIAGNOSTIC
# --------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "SolveEase 300-Target Task Engine API",
        "version": "1.8.0",
        "status": "running",
        "database": "Neon PostgreSQL",
        "health_endpoint": "/api/health",
        "documentation": "/docs",
    }


# --------------------------------------------------
# API AUTHENTICATION
# --------------------------------------------------

def verify_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    if not API_KEY:
        raise HTTPException(
            status_code=503,
            detail="API unavailable: ENGINE_API_KEY is not configured.",
        )

    if credentials is None or not hmac.compare_digest(
        credentials.credentials,
        API_KEY,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return True


# --------------------------------------------------
# VALIDATION MODELS
# --------------------------------------------------

class TargetRegistration(BaseModel):
    platform_name: str = Field(min_length=2, max_length=100)
    api_endpoint: str = Field(min_length=8, max_length=2048)
    extraction_method: str
    tier: int = Field(default=1, ge=1, le=2)

    @field_validator("platform_name")
    @classmethod
    def validate_name(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Platform name cannot be empty.")
        return value

    @field_validator("extraction_method")
    @classmethod
    def validate_method(cls, value):
        value = value.strip().lower()
        if value not in ALLOWED_METHODS:
            raise ValueError("Method must be httpx, playwright, or websocket.")
        return value

    @field_validator("api_endpoint")
    @classmethod
    def validate_endpoint(cls, value):
        value = value.strip()
        try:
            parsed = urlparse(value)
            hostname = parsed.hostname
        except ValueError as exc:
            raise ValueError("Endpoint URL is malformed.") from exc

        if parsed.scheme not in {"https", "wss"}:
            raise ValueError("Endpoint must use HTTPS or WSS.")
        if not hostname or parsed.username or parsed.password:
            raise ValueError("Endpoint must have a hostname and no embedded credentials.")
        return value


class TargetStatusUpdate(BaseModel):
    is_active: bool


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.get("/api/health")
def health():
    try:
        with connect_database() as conn:
            conn.execute("SELECT 1").fetchone()
            res = conn.execute(
                """
                SELECT 
                    to_regclass('public.targets') AS targets_table,
                    to_regclass('public.tasks') AS tasks_table,
                    to_regclass('public.task_events') AS events_table
                """
            ).fetchone()

            if not res or not res.get("targets_table") or not res.get("tasks_table") or not res.get("events_table"):
                raise HTTPException(
                    status_code=503,
                    detail="Database connected, but required tables are missing.",
                )

        return {"status": "ok", "database": "connected", "schema": "verified"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable -> {str(exc)}",
        ) from exc


# --------------------------------------------------
# SUMMARY & QUEUE ENDPOINTS
# --------------------------------------------------

@app.get("/api/summary", dependencies=[Depends(verify_api_key)])
def get_summary():
    result = query_db(
        """
        SELECT
            (SELECT COUNT(*) FROM targets) AS targets,
            (SELECT COUNT(*) FROM targets WHERE is_active = TRUE) AS active_targets,
            (SELECT COUNT(*) FROM tasks WHERE status = 'pending') AS pending,
            (SELECT COUNT(*) FROM tasks WHERE status = 'processing') AS processing,
            (SELECT COUNT(*) FROM tasks WHERE status = 'completed') AS completed,
            (SELECT COUNT(*) FROM tasks WHERE status = 'failed') AS failed
        """
    )
    return result[0]


@app.get("/api/queue/top", dependencies=[Depends(verify_api_key)])
def get_top_tasks(limit: int = Query(default=10, ge=1, le=100)):
    tasks = query_db(
        """
        SELECT task_id, source_platform, category, payout_usd, est_seconds, roc_score, status, created_at
        FROM tasks
        WHERE status = 'pending' AND available_at <= NOW()
        ORDER BY roc_score DESC, created_at ASC, id ASC
        LIMIT %s
        """,
        (limit,),
    )
    return {"count": len(tasks), "top_queue": tasks}


@app.get("/api/targets", dependencies=[Depends(verify_api_key)])
def get_targets():
    targets = query_db(
        """
        SELECT platform_name, api_endpoint, extraction_method, tier, is_active, 
               consecutive_empty_polls, consecutive_failures, last_checked, cooldown_until
        FROM targets
        ORDER BY tier, platform_name
        """
    )
    return {"count": len(targets), "targets": targets}


# --------------------------------------------------
# MANAGEMENT & TRIGGER ENDPOINTS
# --------------------------------------------------

@app.post("/api/targets/register", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)])
def register_target(target: TargetRegistration):
    try:
        with connect_database() as conn:
            inserted = conn.execute(
                """
                INSERT INTO targets (platform_name, api_endpoint, extraction_method, tier, is_active)
                VALUES (%s, %s, %s, %s, FALSE)
                ON CONFLICT (platform_name) DO NOTHING
                RETURNING platform_name
                """,
                (target.platform_name, target.api_endpoint, target.extraction_method, target.tier),
            ).fetchone()

            if not inserted:
                raise HTTPException(status_code=409, detail="Target already exists.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Registration failed: {str(exc)}") from exc

    return {"status": "registered", "platform_name": target.platform_name, "is_active": False}


@app.post("/api/targets/bulk-register", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)])
def bulk_register_targets(targets: list[TargetRegistration]):
    registered, skipped = 0, 0
    try:
        with connect_database() as conn:
            for target in targets:
                inserted = conn.execute(
                    """
                    INSERT INTO targets (platform_name, api_endpoint, extraction_method, tier, is_active)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (platform_name) DO NOTHING
                    RETURNING platform_name
                    """,
                    (target.platform_name, target.api_endpoint, target.extraction_method, target.tier),
                ).fetchone()
                if inserted:
                    registered += 1
                else:
                    skipped += 1
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Bulk registration failed: {str(exc)}") from exc

    return {"status": "completed", "registered": registered, "skipped": skipped, "total": len(targets)}


@app.patch("/api/targets/{platform_name}/status", dependencies=[Depends(verify_api_key)])
def set_target_status(platform_name: str, status_update: TargetStatusUpdate):
    try:
        with connect_database() as conn:
            updated = conn.execute(
                """
                UPDATE targets SET is_active = %s WHERE platform_name = %s
                RETURNING platform_name, is_active
                """,
                (status_update.is_active, platform_name),
            ).fetchone()
            if not updated:
                raise HTTPException(status_code=404, detail=f"Target '{platform_name}' not found.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Status update failed: {str(exc)}") from exc

    return {"status": "updated", "platform_name": updated["platform_name"], "is_active": updated["is_active"]}


@app.post("/api/engine/trigger", dependencies=[Depends(verify_api_key)])
def trigger_engine_poll():
    """Manual on-demand trigger endpoint to force an immediate scan cycle."""
    try:
        result = execute_engine_scan_cycle()
        return {"status": "triggered", **result, "message": "Manual engine scan completed successfully."}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Engine trigger failed: {str(exc)}") from exc
