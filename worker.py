
import asyncio
import json
import logging
import math
import os
import random
import re
from urllib.parse import urlparse

import httpx
import psycopg
import websockets
from psycopg.rows import dict_row


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

POLL_INTERVAL = max(5, int(os.getenv("POLL_INTERVAL", "15")))
REQUEST_TIMEOUT = max(3, int(os.getenv("REQUEST_TIMEOUT", "15")))

EMPTY_POLL_THRESHOLD = 6
FAILURE_THRESHOLD = 3
MAX_CONCURRENT_TARGETS = 10

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is missing. Configure your Neon connection "
        "string in the worker service environment."
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("task-worker")


# --------------------------------------------------
# NEON POSTGRESQL
# --------------------------------------------------

def connect_db():
    """Open a PostgreSQL connection to Neon."""
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=10,
        row_factory=dict_row,
    )


def validate_endpoint(url, method):
    """Accept approved secure schemes; reject unsafe URL forms."""
    parsed = urlparse(url)

    allowed = {
        "httpx": {"https"},
        "playwright": {"https"},
        "websocket": {"wss"},
    }

    if method not in allowed:
        raise ValueError(f"Unsupported extraction method: {method}")

    if parsed.scheme not in allowed[method] or not parsed.hostname:
        raise ValueError(
            "Endpoint must use an approved secure scheme."
        )

    if parsed.username or parsed.password:
        raise ValueError(
            "Credentials must not be embedded in URLs."
        )

    if parsed.hostname.lower() in {
        "localhost",
        "localhost.localdomain",
    }:
        raise ValueError("Localhost targets are not permitted.")

    # Reject private/non-public IP literals.
    # DNS hostnames need separate DNS and outbound-network validation.
    import ipaddress

    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        address = None

    if address is not None and not address.is_global:
        raise ValueError("Non-public IP addresses are not permitted.")

    return url


# --------------------------------------------------
# TARGET MANAGEMENT
# --------------------------------------------------

def load_targets(tier):
    """Load active targets whose cooldown has expired."""
    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                platform_name,
                api_endpoint,
                extraction_method,
                tier,
                consecutive_empty_polls,
                consecutive_failures
            FROM targets
            WHERE tier = %s
              AND is_active = TRUE
              AND (
                  cooldown_until IS NULL
                  OR cooldown_until <= NOW()
              )
            ORDER BY consecutive_failures ASC, id ASC
            """,
            (tier,),
        ).fetchall()

        return [dict(row) for row in rows]


# --------------------------------------------------
# AUDIT EVENTS
# --------------------------------------------------

def record_event(conn, task_row_id, event_type, details=None):
    conn.execute(
        """
        INSERT INTO task_events (
            task_id, event_type, details
        )
        VALUES (%s, %s, %s)
        """,
        (task_row_id, event_type, details),
    )


# --------------------------------------------------
# TASK NORMALIZATION
# --------------------------------------------------

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
            # Stable IDs prevent repeated polls creating duplicates.
            continue

        try:
            payout = float(
                item.get("payout_usd", item.get("payout", 0))
            )
            seconds = int(item.get("est_seconds", 30))
        except (TypeError, ValueError, OverflowError):
            continue

        if not math.isfinite(payout) or payout < 0 or seconds <= 0:
            continue

        payload = item.get("payload", {})

        if not isinstance(payload, str):
            payload = json.dumps(
                payload,
                ensure_ascii=False,
                default=str,
            )

        result.append({
            "task_id": f"{platform_name}:{str(task_id).strip()}",
            "platform": platform_name,
            "category": str(item.get("category", "general"))[:200],
            "payload": payload,
            "payout": payout,
            "seconds": seconds,
        })

    return result


# --------------------------------------------------
# OPTIONAL SOURCE AUTHENTICATION
# --------------------------------------------------

def auth_headers(platform_name):
    """Read optional source tokens from environment variables."""
    slug = re.sub(
        r"[^A-Za-z0-9]", "_", platform_name
    ).upper()

    token = os.getenv(f"TARGET_{slug}_TOKEN")

    headers = {"Accept": "application/json"}

    if token:
        headers["Authorization"] = f"Bearer {token}"

    return headers


# --------------------------------------------------
# HTTPX FEED
# --------------------------------------------------

async def fetch_httpx(target, client):
    url = validate_endpoint(
        target["api_endpoint"],
        target["extraction_method"],
    )

    response = await client.get(
        url,
        headers=auth_headers(target["platform_name"]),
    )

    if response.status_code == 429:
        raise RuntimeError("Source rate-limited the request (HTTP 429).")

    response.raise_for_status()

    return normalize_tasks(
        response.json(),
        target["platform_name"],
    )


# --------------------------------------------------
# PLAYWRIGHT FEED
# --------------------------------------------------

async def fetch_playwright(target):
    """Read JSON responses exposed by an authorized website."""
    from playwright.async_api import async_playwright

    url = validate_endpoint(
        target["api_endpoint"],
        target["extraction_method"],
    )

    found = []
    allowed_host = urlparse(url).hostname

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        try:
            context = await browser.new_context()
            page = await context.new_page()

            async def on_response(response):
                if response.status != 200:
                    return

                if urlparse(response.url).hostname != allowed_host:
                    return

                content_type = response.headers.get(
                    "content-type", ""
                ).lower()

                if "json" not in content_type:
                    return

                try:
                    data = await response.json()
                    found.extend(
                        normalize_tasks(
                            data,
                            target["platform_name"],
                        )
                    )
                except (ValueError, TypeError):
                    log.debug(
                        "Ignored unsupported JSON response from %s",
                        target["platform_name"],
                    )

            page.on("response", on_response)

            try:
                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=REQUEST_TIMEOUT * 1000,
                )
                await page.wait_for_timeout(1500)
            finally:
                page.remove_listener("response", on_response)

        finally:
            await browser.close()

    return found


# --------------------------------------------------
# WEBSOCKET FEED
# --------------------------------------------------

async def fetch_websocket(target):
    """Read one message from an authorized WSS feed."""
    url = validate_endpoint(
        target["api_endpoint"],
        target["extraction_method"],
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
                ws.recv(),
                timeout=REQUEST_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return []

    data = json.loads(message)

    return normalize_tasks(
        data,
        target["platform_name"],
    )


# --------------------------------------------------
# SAVE AND DEDUPLICATE TASKS
# --------------------------------------------------

def save_tasks(target, tasks):
    """Insert new tasks and audit events in one transaction."""
    inserted = 0

    with connect_db() as conn:
        for task in tasks:
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
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (task_id) DO NOTHING
                RETURNING id
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
            ).fetchone()

            if row is None:
                continue

            inserted += 1

            record_event(
                conn,
                row["id"],
                "ingested",
                f"Source tier {target['tier']}",
            )

        # Reset the empty-poll counter if new tasks were added.
        # Otherwise, increment it instead of repeatedly setting it to 1.
        conn.execute(
            """
            UPDATE targets
            SET consecutive_empty_polls =
                    CASE
                        WHEN %s > 0 THEN 0
                        ELSE consecutive_empty_polls + 1
                    END,
                consecutive_failures = 0,
                last_checked = NOW(),
                cooldown_until = NULL
            WHERE id = %s
            """,
            (inserted, target["id"]),
        )

    return inserted


# --------------------------------------------------
# FAILURE TRACKING
# --------------------------------------------------

def record_failure(target, error):
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE targets
            SET consecutive_failures = consecutive_failures + 1,
                last_checked = NOW(),
                cooldown_until = CASE
                    WHEN consecutive_failures + 1 >= %s
                    THEN NOW() + INTERVAL '60 seconds'
                    ELSE cooldown_until
                END
            WHERE id = %s
            """,
            (FAILURE_THRESHOLD, target["id"]),
        )

    log.warning(
        "[%s] Poll failed: %s",
        target["platform_name"],
        str(error)[:300],
    )


# --------------------------------------------------
# POLLING
# --------------------------------------------------

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
                raise ValueError(
                    f"Unsupported extraction method: {method}"
                )

            inserted = save_tasks(target, tasks)

            log.info(
                "[%s] Received %d; inserted %d new tasks",
                target["platform_name"],
                len(tasks),
                inserted,
            )

        except Exception as exc:
            try:
                record_failure(target, exc)
            except Exception:
                log.exception(
                    "[%s] Could not record source failure",
                    target["platform_name"],
                )


# --------------------------------------------------
# SMART FAILOVER
# --------------------------------------------------

def choose_tier(primary):
    """Use backups when all eligible primary sources are unhealthy."""
    if not primary:
        return 2

    primary_unhealthy = all(
        target["consecutive_empty_polls"] >= EMPTY_POLL_THRESHOLD
        or target["consecutive_failures"] >= FAILURE_THRESHOLD
        for target in primary
    )

    return 2 if primary_unhealthy else 1


async def run_smart_failover_engine():
    log.info("SolveEase task ingestion worker starting")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TARGETS)

    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT_TARGETS,
        max_keepalive_connections=MAX_CONCURRENT_TARGETS,
    )

    timeout = httpx.Timeout(REQUEST_TIMEOUT)

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        follow_redirects=False,
    ) as client:

        while True:
            try:
                primary = load_targets(1)
                tier = choose_tier(primary)
                targets = load_targets(tier)

                # If no backup source is available, return to primary
                # sources that remain active and outside cooldown.
                if not targets and tier == 2:
                    targets = primary
                    tier = 1

                log.info(
                    "Polling tier %d (%d eligible targets)",
                    tier,
                    len(targets),
                )

                await asyncio.gather(*(
                    poll_target(target, client, semaphore)
                    for target in targets
                ))

            except psycopg.Error:
                log.exception(
                    "Neon PostgreSQL error in polling loop"
                )

            except Exception:
                log.exception("Unexpected worker-loop error")

            await asyncio.sleep(
                POLL_INTERVAL + random.uniform(0, 2)
            )


if __name__ == "__main__":
    try:
        asyncio.run(run_smart_failover_engine())
    except KeyboardInterrupt:
        log.info("Worker stopped")
