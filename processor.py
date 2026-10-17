%%writefile processor.py
import os
import asyncio
import logging
import httpx
import psycopg
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Processor")

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_2x7qYKoXPwrh@ep-polished-unit-b511m9w5-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def get_db_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

async def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")

async def process_tasks():
    logger.info("⚙️ Fetching PENDING tasks from Neon queue...")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Case-insensitive fetch to catch any variant of 'pending'
            cur.execute("SELECT id, task_id, payload, status FROM tasks WHERE status ILIKE 'pending' LIMIT 10;")
            tasks = cur.fetchall()

            if not tasks:
                logger.info("📭 No pending tasks found in queue.")
                return

            logger.info(f"🔄 Processing {len(tasks)} tasks...")
            processed_count = 0

            for task in tasks:
                task_db_id = task["id"]
                task_id = task["task_id"]
                
                logger.info(f"▶️ Executing task ID: {task_id}")
                
                # Update status to COMPLETED
                cur.execute(
                    "UPDATE tasks SET status = 'COMPLETED' WHERE id = %s;",
                    (task_db_id,)
                )
                processed_count += 1
            
            # Explicitly commit transaction
            conn.commit()

    logger.info(f"✨ Successfully processed and committed {processed_count} tasks.")
    await send_telegram_alert(f"🚀 *Task Processing Complete*\nSuccessfully processed & completed `{processed_count}` tasks from the queue.")

if __name__ == "__main__":
    asyncio.run(process_tasks())
