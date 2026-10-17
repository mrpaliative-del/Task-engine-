
import asyncio
import json
import logging
import os
import random
import sqlite3
from urllib.parse import urlparse

import httpx
import websockets

DB_FILE = os.getenv("DB_FILE", "task_engine.db")
POLL_INTERVAL = max(5, int(os.getenv("POLL_INTERVAL", "15")))
REQUEST_TIMEOUT = max(3, int(os.getenv("REQUEST_TIMEOUT", "15")))
EMPTY_POLL_THRESHOLD = 6
FAILURE_THRESHOLD = 3
MAX_CONCURRENT_TARGETS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("task-worker")


def connect_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def validate_endpoint(url, method):
    """Accept HTTPS APIs and WSS feeds; reject unsafe schemes."""
    parsed = urlparse(url)

    allowed = {
        "httpx": {"https"},
        "playwright": {"https"},
        "websocket": {"wss"},
    }

    if method not in allowed:
        raise ValueError(f"Unsupported extraction method: {method}")

    if parsed.scheme not in allowed[method] or not parsed.hostname:
        raise ValueError("Endpoint must use an approved secure scheme")

    if parsed.username or parsed.password:
        raise ValueError("Credentials must not be embedded in URLs")

    return url


def load_targets(tier):
    with connect_db() as conn:
        return [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, platform_name, api_endpoint,
                       extraction_method, tier,
                       consecutive_empty_polls,
                       consecutive_failures
                FROM targets
                WHERE tier = ? AND is_active = 1
                ORDER BY consecutive_failures ASC, id ASC
                """,
                (tier,),
            ).fetchall()
        ]


def record_event(conn, task_row_id, event_type, details=None):
    conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, details)
        VALUES (?, ?, ?)
        """,
        (task_row_id, event_type, details),
    )


def normalize_tasks(data, platform_name):
    """Normalize supported feed shapes and reject invalid records."""
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("tasks")
        if items is None:
            items = data.get("data", [])
        if isinstance(items, dict):
            items = items.get("tasks", [])
    else:
        return []

    if not isinstance(items, list):
        return []

    result = []

    for item in items:
        if not isinstance(item, dict):
            continue

        task_id = item.get("id") or item.get("task_id")
        if task_id is None or not str(task_id).strip():
            # Stable IDs are required to deduplicate repeated polls.
            continue

        try:
            payout = float(item.get("payout_usd", item.get("payout", 0)))
            seconds = int(item.get("est_seconds", 30))
        except (TypeError, ValueError):
            continue

        if payout < 0 or seconds <= 0:
            continue

        if payout != payout or payout == float("inf"):
            continue

        payload = item.get("payload", {})
        if not isinstance(payload, str):
            payload = json.dumps(
                payload, ensure_ascii=False, default=str
            )

        result.append({
            "task_id": f"{platform_name}:{task_id}",
            "platform": platform_name,
            "category": str(item.get("category", "general"))[:200],
            "payload": payload,
            "payout": payout,
            "seconds": seconds,
        })

    return result


def auth_headers(platform_name):
    """Optional bearer token; never store secrets in SQLite."""
    import re

    slug = re.sub(r"[^A-Za-z0-9]", "_", platform_name).upper()
    token = os.getenv(f"TARGET_{slug}_TOKEN")

    headers = {"Accept": "application/json"}

    if token:
        headers["Authorization"] = f"Bearer {token}"

    return headers


async def fetch_httpx(target, client):
    url = validate_endpoint(
        target["api_endpoint"], target["extraction_method"]
    )

    response = await client.get(
        url,
        headers=auth_headers(target["platform_name"]),
    )

    if response.status_code == 429:
        raise RuntimeError("Rate limited (HTTP 429)")

    response.raise_for_status()
    return normalize_tasks(response.json(), target["platform_name"])


async def fetch_playwright(target):
    """Read task data exposed by an authorized page/API response."""
    from playwright.async_api import async_playwright

    url = validate_endpoint(
        target["api_endpoint"], target["extraction_method"]
    )
    found = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context()
            page = await context.new_page()

            async def on_response(response):
                if response.status != 200:
                    return
                if urlparse(response.url).hostname != urlparse(url).hostname:
                    return
                if "json" not in response.headers.get("content-type", ""):
                    return

                try:
                    data = await response.json()
                    found.extend(
                        normalize_tasks(data, target["platform_name"])
                    )
                except (ValueError, TypeError):
                    log.debug("Ignored a non-JSON or unsupported response")

            page.on("response", on_response)
            await page.goto(
                url, wait_until="domcontentloaded",
                timeout=REQUEST_TIMEOUT * 1000,
            )
            await page.wait_for_timeout(1500)
            await page.remove_listener("response", on_response)
        finally:
            await browser.close()

    return found


async def fetch_websocket(target):
    """Read one message from an authorized WSS task feed."""
    url = validate_endpoint(
        target["api_endpoint"], target["extraction_method"]
    )

    headers = auth_headers(target["platform_name"])

    async with websockets.connect(
        url,
        extra_headers=headers,
        open_timeout=REQUEST_TIMEOUT,
        close_timeout=5,
        ping_timeout=20,
        max_size=2 * 1024 * 1024,
    ) as ws:
        try:
            message = await asyncio.wait_for(
                ws.recv(), timeout=REQUEST_TIMEOUT
            )
        except asyncio.TimeoutError:
            return []

    data = json.loads(message)
    return normalize_tasks(data, target["platform_name"])


def save_tasks(target, tasks):
    inserted = 0

    with connect_db() as conn:
        for task in tasks:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO tasks (
                    task_id, source_target_id, source_platform,
                    category, payload, payout_usd, est_seconds, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    task["task_id"],
                    target["id"],
                    task["platform"],
                    task["category"],
                    task["payload"],
                    task["payout"],
                    task["seconds"],
                ),
            )

            if cursor.rowcount:
                inserted += 1
                task_row_id = cursor.lastrowid
                record_event(
                    conn, task_row_id, "ingested",
                    f"Source tier {target['tier']}",
                )

        conn.execute(
            """
            UPDATE targets
            SET consecutive_empty_polls = ?,
                consecutive_failures = 0,
                last_checked = CURRENT_TIMESTAMP,
                cooldown_until = NULL
            WHERE id = ?
            """,
            (0 if inserted else 1, target["id"]),
        )

    return inserted


def record_failure(target, error):
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE targets
            SET consecutive_failures = consecutive_failures + 1,
                last_checked = CURRENT_TIMESTAMP,
                cooldown_until = CASE
                    WHEN consecutive_failures + 1 >= ?
                    THEN datetime('now', '+60 seconds')
                    ELSE cooldown_until
                END
            WHERE id = ?
            """,
            (FAILURE_THRESHOLD, target["id"]),
        )

    log.warning(
        "[%s] Poll failed: %s",
        target["platform_name"],
        str(error)[:300],
    )


async def poll_target(target, client, semaphore):
    async with semaphore:
        try:
            method = target["extraction_method"]

            if method == "httpx":
                tasks = await fetch_httpx(target, client)
            elif method == "playwright":
                tasks = await fetch_playwright(target)
            elif method == "websocket":
                tasks = await fetch_websocket(target)
            else:
                raise ValueError(f"Unsupported method: {method}")

            inserted = save_tasks(target, tasks)
            log.info(
                "[%s] Received %d; inserted %d new tasks",
                target["platform_name"], len(tasks), inserted,
            )

        except Exception as exc:
            record_failure(target, exc)


def choose_tier(primary):
    """Use backup when all active primary feeds are dry or failing."""
    if not primary:
        return 2

    primary_unhealthy = all(
        t["consecutive_empty_polls"] >= EMPTY_POLL_THRESHOLD
        or t["consecutive_failures"] >= FAILURE_THRESHOLD
        for t in primary
    )

    return 2 if primary_unhealthy else 1


async def run_smart_failover_engine():
    log.info("Task ingestion worker starting")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TARGETS)
    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT_TARGETS,
        max_keepalive_connections=MAX_CONCURRENT_TARGETS,
    )

    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT,
        limits=limits,
        follow_redirects=False,
    ) as client:
        while True:
            try:
                primary = load_targets(1)
                tier = choose_tier(primary)
                targets = load_targets(tier)

                if not targets and tier == 2:
                    targets = primary
                    tier = 1

                log.info("Polling tier %d (%d targets)", tier, len(targets))

                await asyncio.gather(*(
                    poll_target(t, client, semaphore)
                    for t in targets
                ))

            except sqlite3.Error:
                log.exception("Database error in polling loop")
            except Exception:
                log.exception("Unexpected worker-loop error")

            await asyncio.sleep(POLL_INTERVAL + random.uniform(0, 2))


if __name__ == "__main__":
    try:
        asyncio.run(run_smart_failover_engine())
    except KeyboardInterrupt:
        log.info("Worker stopped")
