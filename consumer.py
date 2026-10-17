
import asyncio
import json
import logging
import os
import random
import sqlite3
from datetime import datetime, timezone

import httpx

DB_FILE = os.getenv("DB_FILE", "task_engine.db")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() in {
    "1", "true", "yes"
}
SUBMISSION_URL = os.getenv("TASK_SUBMISSION_URL", "").strip()
SUBMISSION_TOKEN = os.getenv("TASK_SUBMISSION_TOKEN", "")
POLL_SECONDS = 2
REQUEST_TIMEOUT = 15
MAX_BACKOFF_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("task-consumer")


def connect_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def record_event(conn, db_id, event_type, details=None):
    conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, details)
        VALUES (?, ?, ?)
        """,
        (db_id, event_type, details),
    )


def fetch_and_lock_top_task():
    """Atomically claim one eligible task; safe for competing consumers."""
    conn = connect_db()

    try:
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            """
            SELECT id, task_id, source_platform, category, payload,
                   payout_usd, est_seconds, roc_score,
                   attempts, max_attempts
            FROM tasks
            WHERE status = 'pending'
              AND attempts < max_attempts
              AND available_at <= CURRENT_TIMESTAMP
            ORDER BY roc_score DESC, created_at ASC, id ASC
            LIMIT 1
            """
        ).fetchone()

        if row is None:
            conn.commit()
            return None

        cursor = conn.execute(
            """
            UPDATE tasks
            SET status = 'processing',
                attempts = attempts + 1,
                locked_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                last_error = NULL
            WHERE id = ? AND status = 'pending'
            """,
            (row["id"],),
        )

        if cursor.rowcount != 1:
            conn.rollback()
            return None

        record_event(
            conn,
            row["id"],
            "claimed",
            f"Attempt {row['attempts'] + 1}",
        )

        task = dict(row)
        task["attempts"] += 1

        conn.commit()
        return task

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_dry_run(task):
    """Release a test claim without falsely recording task completion."""
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE tasks
            SET status = 'pending',
                attempts = MAX(0, attempts - 1),
                locked_at = NULL,
                available_at = datetime('now', '+60 seconds'),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND status = 'processing'
            """,
            (task["id"],),
        )
        record_event(
            conn,
            task["id"],
            "dry_run",
            "No external submission performed",
        )

    log.info("[DRY RUN] Task %s was not submitted.", task["task_id"])


def mark_completed(task, details):
    with connect_db() as conn:
        cursor = conn.execute(
            """
            UPDATE tasks
            SET status = 'completed',
                locked_at = NULL,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                last_error = NULL
            WHERE id = ? AND status = 'processing'
            """,
            (task["id"],),
        )

        if cursor.rowcount == 1:
            record_event(
                conn, task["id"], "completed", details[:1000]
            )

    log.info("[COMPLETED] %s", task["task_id"])


def mark_failure(task, error, retryable=True):
    message = str(error)[:1000]
    can_retry = retryable and task["attempts"] < task["max_attempts"]

    delay = min(
        MAX_BACKOFF_SECONDS,
        2 ** min(task["attempts"], 8),
    ) + random.uniform(0, 1)

    with connect_db() as conn:
        if can_retry:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    locked_at = NULL,
                    available_at = datetime('now', ?),
                    updated_at = CURRENT_TIMESTAMP,
                    last_error = ?
                WHERE id = ? AND status = 'processing'
                """,
                (f"+{int(delay)} seconds", message, task["id"]),
            )
            event = "retry_scheduled"
        else:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'failed',
                    locked_at = NULL,
                    updated_at = CURRENT_TIMESTAMP,
                    last_error = ?
                WHERE id = ? AND status = 'processing'
                """,
                (message, task["id"]),
            )
            event = "failed"

        record_event(conn, task["id"], event, message)

    log.warning(
        "[%s] %s: %s",
        event.upper(), task["task_id"], message,
    )


async def submit_task(task, client):
    """Submit only to an explicitly configured, authorized endpoint."""
    if DRY_RUN:
        mark_dry_run(task)
        return

    if not SUBMISSION_URL.startswith("https://"):
        mark_failure(
            task,
            "No valid HTTPS TASK_SUBMISSION_URL configured",
            retryable=False,
        )
        return

    try:
        payload = json.loads(task["payload"])
    except (json.JSONDecodeError, TypeError):
        payload = {"raw_payload": task["payload"]}

    body = {
        "task_id": task["task_id"],
        "source_platform": task["source_platform"],
        "category": task["category"],
        "payload": payload,
    }

    headers = {"Accept": "application/json"}
    if SUBMISSION_TOKEN:
        headers["Authorization"] = f"Bearer {SUBMISSION_TOKEN}"

    try:
        response = await client.post(
            SUBMISSION_URL,
            json=body,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code in (200, 201, 202):
            # This records HTTP acceptance, not proof of external
            # business completion unless the provider guarantees it.
            mark_completed(
                task,
                f"Provider HTTP status {response.status_code}",
            )
        elif response.status_code == 429 or response.status_code >= 500:
            mark_failure(
                task, f"Retryable HTTP {response.status_code}"
            )
        else:
            mark_failure(
                task,
                f"Non-retryable HTTP {response.status_code}",
                retryable=False,
            )

    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        # A timeout can occur after the server accepted the request.
        # Retry only if the provider supports idempotency keys.
        mark_failure(
            task,
            f"Network outcome uncertain: {exc}. "
            "Verify provider idempotency before retrying.",
            retryable=False,
        )
    except Exception as exc:
        log.exception("Unexpected submission error")
        mark_failure(task, f"Unexpected error: {exc}")


async def execution_loop():
    log.info("Consumer started; dry-run=%s", DRY_RUN)

    async with httpx.AsyncClient(
        follow_redirects=False,
        timeout=REQUEST_TIMEOUT,
    ) as client:
        while True:
            try:
                task = await asyncio.to_thread(
                    fetch_and_lock_top_task
                )

                if task is None:
                    await asyncio.sleep(POLL_SECONDS)
                    continue

                log.info(
                    "Claimed %s | RoC %.6f USD/sec",
                    task["task_id"],
                    task["roc"],
                )

                await submit_task(task, client)

            except sqlite3.Error:
                log.exception("Database error")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Consumer loop error")
                await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(execution_loop())
    except KeyboardInterrupt:
        log.info("Consumer stopped")
