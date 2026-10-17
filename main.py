
import os
import sqlite3
from contextlib import closing

from fastapi import FastAPI, HTTPException

app = FastAPI(
    title="SolveEase 300-Target Task Engine API",
    version="1.0.0",
    description="Read-only task queue and system telemetry.",
)

DB_FILE = os.getenv("DB_FILE", "task_engine.db")


def query_db(sql, params=()):
    if not os.path.isfile(DB_FILE):
        raise HTTPException(
            status_code=503,
            detail="Database file not found. Initialize schema.sql first.",
        )

    try:
        with closing(
            sqlite3.connect(DB_FILE, timeout=10)
        ) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 10000")
            return [
                dict(row)
                for row in conn.execute(sql, params).fetchall()
            ]
    except sqlite3.Error as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable: {exc}",
        ) from exc


@app.get("/api/health")
def health():
    query_db("SELECT 1 AS ok")
    return {"status": "ok", "database": "connected"}


@app.get("/api/summary")
def get_summary():
    rows = query_db("""
        SELECT
            (SELECT COUNT(*) FROM targets) AS total_targets,
            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1) AS active_targets,
            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1 AND tier = 1) AS active_tier1,
            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1 AND tier = 2) AS active_tier2,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'pending') AS pending_tasks,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'processing') AS processing_tasks,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'completed') AS completed_tasks,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'failed') AS failed_tasks
    """)
    return rows[0]


@app.get("/api/queue/top")
def get_top_tasks(limit: int = 10):
    limit = max(1, min(limit, 100))

    tasks = query_db("""
        SELECT
            task_id, source_platform, category,
            payout_usd, est_seconds, roc_score,
            status, created_at
        FROM tasks
        WHERE status = 'pending'
          AND available_at <= CURRENT_TIMESTAMP
        ORDER BY roc_score DESC, created_at ASC, id ASC
        LIMIT ?
    """, (limit,))

    return {"count": len(tasks), "top_queue": tasks}


@app.get("/api/targets")
def get_targets():
    targets = query_db("""
        SELECT
            platform_name, tier, extraction_method,
            is_active, consecutive_empty_polls,
            consecutive_failures, last_checked,
            cooldown_until
        FROM targets
        ORDER BY tier ASC, platform_name ASC
    """)
    return {"count": len(targets), "targets": targets}


@app.get("/api/tasks/recent")
def get_recent_tasks(limit: int = 25):
    limit = max(1, min(limit, 100))

    tasks = query_db("""
        SELECT
            task_id, source_platform, category,
            payout_usd, est_seconds, roc_score,
            status, attempts, last_error,
            created_at, updated_at, completed_at
        FROM tasks
        ORDER BY updated_at DESC, id DESC
        LIMIT ?
    """, (limit,))

    return {"count": len(tasks), "tasks": tasks}
