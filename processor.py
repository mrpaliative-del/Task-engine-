%%writefile processor.py
import os
import asyncio
import logging

import httpx
import psycopg
from psycopg.rows import dict_row

logging.basicConfig(
level=logging.INFO,
format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("SolveEaseProcessor")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

def get_db_connection():
if not DATABASE_URL:
raise RuntimeError("DATABASE_URL environment variable is missing")

return psycopg.connect(
    DATABASE_URL,
    connect_timeout=10,
    row_factory=dict_row,
)

async def send_telegram_alert(message: str):
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
logger.info("Telegram not configured; skipping notification.")
return

url = (
    f"https://api.telegram.org/"
    f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
)

try:
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
            },
        )
        response.raise_for_status()

        if not response.json().get("ok"):
            raise RuntimeError("Telegram rejected the notification")

    logger.info("Telegram notification accepted.")

except Exception as exc:
    # Do not log the request URL because it contains the bot token.
    logger.error(
        "Telegram notification failed: %s",
        type(exc).__name__,
    )

def inspect_queue():
logger.info("Connecting to Neon and inspecting the task queue...")

with get_db_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (
                    WHERE status ILIKE 'pending'
                ) AS pending_count,
                COUNT(*) FILTER (
                    WHERE status ILIKE 'processing'
                ) AS processing_count,
                COUNT(*) FILTER (
                    WHERE status ILIKE 'completed'
                ) AS completed_count,
                COUNT(*) FILTER (
                    WHERE status ILIKE 'failed'
                ) AS failed_count
            FROM tasks;
        """)

        counts = cur.fetchone()

        cur.execute("""
            SELECT task_id, status, created_at
            FROM tasks
            WHERE status ILIKE 'pending'
            ORDER BY created_at ASC
            LIMIT 10;
        """)

        pending_tasks = cur.fetchall()

return counts, pending_tasks

async def main():
try:
counts, pending_tasks = inspect_queue()

    logger.info(
        "Queue: pending=%s processing=%s completed=%s failed=%s",
        counts["pending_count"],
        counts["processing_count"],
        counts["completed_count"],
        counts["failed_count"],
    )

    lines = [
        "SolveEase Task Queue Report",
        f"Pending: {counts['pending_count']}",
        f"Processing: {counts['processing_count']}",
        f"Completed: {counts['completed_count']}",
        f"Failed: {counts['failed_count']}",
        "",
        "Monitoring only: no tasks were executed or modified.",
    ]

    if pending_tasks:
        lines.extend(["", "Oldest pending tasks:"])
        for task in pending_tasks:
            lines.append(
                f"- {task['task_id']} ({task['status']})"
            )

    await send_telegram_alert("\n".join(lines))

except Exception as exc:
    logger.error(
        "Queue inspection failed: exception=%s SQLSTATE=%s",
        type(exc).__name__,
        getattr(exc, "sqlstate", None),
    )
    raise

if name == "main":
asyncio.run(main())
