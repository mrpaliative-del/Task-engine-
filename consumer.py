
import asyncio
import json
import logging
import math
import os
import random

import httpx
import psycopg
from psycopg.rows import dict_row


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in {
    "1", "true", "yes"
}

SUBMISSION_URL = os.getenv("TASK_SUBMISSION_URL", "").strip()
SUBMISSION_TOKEN = os.getenv("TASK_SUBMISSION_TOKEN", "").strip()

POLL_SECONDS = 2
REQUEST_TIMEOUT = 15
MAX_BACKOFF_SECONDS = 300

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is missing. Configure your Neon connection "
        "string in the consumer service environment."
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("task-consumer")


# --------------------------------------------------
# NEON POSTGRESQL
# --------------------------------------------------

def connect_db():
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=10,
        row_factory=dict_row,
    )


def record_event(conn, db_id, event_type, details=None):
    conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, details)
        VALUES (%s, %s, %s)
        """,
        (db_id, event_type, details),
    )


# --------------------------------------------------
# ATOMIC TASK CLAIMING
# --------------------------------------------------

def fetch_and_lock_top_task():
    """
    Claim one task atomically.

    FOR UPDATE SKIP LOCKED allows multiple consumers to work
    concurrently without claiming the same task.
    """
    with connect_db() as conn:
        with conn.transaction():
            row = conn.execute(
                """
                SELECT
                    id,
                    task_id,
                    source_platform,
                    category,
                    payload,
                    payout_usd,
                    est_seconds,
                    roc_score,
                    attempts,
                    max_attempts
                FROM tasks
                WHERE status = 'pending'
                  AND attempts < max_attempts
                  AND available_at <= NOW()
                ORDER BY
                    roc_score DESC,
                    created_at ASC,
                    id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            ).fetchone()

            if row is None:
                return None

            updated = conn.execute(
                """
                UPDATE tasks
                SET status = 'processing',
                    attempts = attempts + 1,
                    locked_at = NOW(),
                    updated_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                  AND status = 'pending'
                RETURNING attempts
                """,
                (row["id"],),
            ).fetchone()

            if updated is None:
                return None

            task = dict(row)
            task["attempts"] = updated["attempts"]

            record_event(
                conn,
                task["id"],
                "claimed",
                f"Attempt {task['attempts']}",
            )

            return task


# --------------------------------------------------
# DRY RUN
# --------------------------------------------------

def mark_dry_run(task):
    """Release a test claim without submitting or completing it."""
    with connect_db() as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    attempts = GREATEST(0, attempts - 1),
                    locked_at = NULL,
                    available_at = NOW() + INTERVAL '60 seconds',
                    updated_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                  AND status = 'processing'
                """,
                (task["id"],),
            )

            record_event(
                conn,
                task["id"],
                "dry_run",
                "No external submission performed",
            )

    log.info(
        "[DRY RUN] %s was not submitted.",
        task["task_id"],
    )


# --------------------------------------------------
# PROVIDER ACCEPTANCE / COMPLETION
# --------------------------------------------------

def mark_provider_accepted(task, details):
    """
    Record provider acceptance without falsely claiming completion.

    The existing schema has no 'submitted' status, so the task
    remains processing until completion is independently confirmed.
    """
    message = details[:1000]

    with connect_db() as conn:
        with conn.transaction():
            conn.execute(
                """
                UPDATE tasks
                SET status = 'processing',
                    locked_at = NULL,
                    updated_at = NOW(),
                    last_error = %s
                WHERE id = %s
                  AND status = 'processing'
                """,
                (
                    "Provider accepted; completion unconfirmed. "
                    + message,
                    task["id"],
                ),
            )

            record_event(
                conn,
                task["id"],
                "provider_accepted",
                message,
            )

    log.info(
        "[ACCEPTED - NOT CONFIRMED COMPLETE] %s",
        task["task_id"],
    )


def mark_completed(task, details):
    with connect_db() as conn:
        with conn.transaction():
            result = conn.execute(
                """
                UPDATE tasks
                SET status = 'completed',
                    locked_at = NULL,
                    completed_at = NOW(),
                    updated_at = NOW(),
                    last_error = NULL
                WHERE id = %s
                  AND status = 'processing'
                RETURNING id
                """,
                (task["id"],),
            ).fetchone()

            if result:
                record_event(
                    conn,
                    task["id"],
                    "completed",
                    details[:1000],
                )

    log.info("[COMPLETED] %s", task["task_id"])


# --------------------------------------------------
# FAILURE AND RETRIES
# --------------------------------------------------

def mark_failure(task, error, retryable=True):
    message = str(error)[:1000]

    can_retry = (
        retryable
        and task["attempts"] < task["max_attempts"]
    )

    delay = min(
        MAX_BACKOFF_SECONDS,
        2 ** min(task["attempts"], 8),
    ) + random.uniform(0, 1)

    with connect_db() as conn:
        with conn.transaction():
            if can_retry:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'pending',
                        locked_at = NULL,
                        available_at =
                            NOW() + (%s * INTERVAL '1 second'),
                        updated_at = NOW(),
                        last_error = %s
                    WHERE id = %s
                      AND status = 'processing'
                    """,
                    (delay, message, task["id"]),
                )

                event = "retry_scheduled"

            else:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'failed',
                        locked_at = NULL,
                        updated_at = NOW(),
                        last_error = %s
                    WHERE id = %s
                      AND status = 'processing'
                    """,
                    (message, task["id"]),
                )

                event = "failed"

            record_event(
                conn,
                task["id"],
                event,
                message,
            )

    log.warning(
        "[%s] %s: %s",
        event.upper(),
        task["task_id"],
        message,
    )


# --------------------------------------------------
# SUBMISSION
# --------------------------------------------------

async def submit_task(task, client):
    if DRY_RUN:
        await asyncio.to_thread(mark_dry_run, task)
        return

    if not SUBMISSION_URL.startswith("https://"):
        await asyncio.to_thread(
            mark_failure,
            task,
            "No valid HTTPS TASK_SUBMISSION_URL configured.",
            False,
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

    # Do not automatically retry uncertain submissions unless the
    # provider supports an idempotency mechanism.
    headers["Idempotency-Key"] = task["task_id"]

    try:
        response = await client.post(
            SUBMISSION_URL,
            json=body,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code in (200, 201, 202):
            provider_status = ""
            explicitly_completed = False

            try:
                result = response.json()

                if isinstance(result, dict):
                    provider_status = str(
                        result.get("status", "")
                    ).strip().lower()

                    explicitly_completed = (
                        result.get("completed") is True
                        or provider_status in {
                            "completed",
                            "complete",
                        }
                    )
            except (ValueError, TypeError):
                result = None

            details = (
                f"Provider HTTP status {response.status_code}; "
                f"provider status={provider_status or 'unspecified'}"
            )

            if explicitly_completed:
                await asyncio.to_thread(
                    mark_completed,
                    task,
                    details,
                )
            else:
                await asyncio.to_thread(
                    mark_provider_accepted,
                    task,
                    details,
                )

        elif response.status_code == 429 or response.status_code >= 500:
            await asyncio.to_thread(
                mark_failure,
                task,
                f"Retryable HTTP {response.status_code}",
                True,
            )

        else:
            await asyncio.to_thread(
                mark_failure,
                task,
                f"Non-retryable HTTP {response.status_code}",
                False,
            )

    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        # The provider may have received the request despite a timeout.
        # Do not blindly retry: reconcile using provider records first.
        await asyncio.to_thread(
            mark_failure,
            task,
            (
                f"Submission outcome uncertain: {type(exc).__name__}. "
                "Check provider records before retrying."
            ),
            False,
        )

    except Exception as exc:
        log.exception("Unexpected submission error")

        await asyncio.to_thread(
            mark_failure,
            task,
            f"Unexpected error: {type(exc).__name__}",
            False,
        )


# --------------------------------------------------
# EXECUTION LOOP
# --------------------------------------------------

async def execution_loop():
    log.info("Consumer started; dry_run=%s", DRY_RUN)

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
                    "Claimed %s | ROC %.8f USD/sec",
                    task["task_id"],
                    float(task["roc_score"]),
                )

                await submit_task(task, client)

            except psycopg.Error:
                log.exception("Neon PostgreSQL error")
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
