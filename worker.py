
"""
SolveEase 300-Target Task Engine — worker.py

Continuous task scanner with:
- HTTP/JSON, Playwright/browser and WebSocket sources
- PostgreSQL task storage and audit events
- Duplicate-task protection
- Generated-column-safe inserts (roc_score is intentionally omitted)
- Empty-poll tracking and tier failover
- Target cooldowns and resilient error handling

Required environment variable:
    DATABASE_URL

Optional environment variables:
    POLL_INTERVAL=15
    REQUEST_TIMEOUT=15
    EMPTY_POLL_THRESHOLD=6
    FAILURE_THRESHOLD=3
    MAX_CONCURRENT_TARGETS=10
    TARGET_<PLATFORM_SLUG>_TOKEN=<source token>

Database tables expected:
    targets, tasks, task_events

Run as a dedicated long-running worker.
"""

import asyncio
import ipaddress
import json
import logging
import math
import os
import random
import re
import socket
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.rows import dict_row


# ============================================================
# CONFIGURATION
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

POLL_INTERVAL = max(5, int(os.getenv("POLL_INTERVAL", "15")))
REQUEST_TIMEOUT = max(5, int(os.getenv("REQUEST_TIMEOUT", "15")))
EMPTY_POLL_THRESHOLD = max(
    1, int(os.getenv("EMPTY_POLL_THRESHOLD", "6"))
)
FAILURE_THRESHOLD = max(1, int(os.getenv("FAILURE_THRESHOLD", "3")))
MAX_CONCURRENT_TARGETS = max(
    1, int(os.getenv("MAX_CONCURRENT_TARGETS", "10"))
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("solveease.worker")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is missing. Configure it in the deployment "
        "environment; do not hard-code database credentials."
    )


# ============================================================
# DATABASE
# ============================================================

def connect_db():
    """Create a PostgreSQL connection."""
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=10,
        row_factory=dict_row,
    )


def database_error(operation, exc):
    """Log database errors without intentionally printing the DB URL."""
    sqlstate = getattr(exc, "sqlstate", None)
    message = str(exc)

    message = re.sub(
        r"(?i)(postgres(?:ql)?://)[^@\s]+@",
        r"\1[REDACTED]@",
        message,
    )
    message = re.sub(
        r"(?i)\b(password|passwd|pwd)\s*=\s*[^;\s]+",
        r"\1=[REDACTED]",
        message,
    )

    logger.error(
        "%s failed: exception=%s SQLSTATE=%s detail=%s",
        operation,
        type(exc).__name__,
        sqlstate,
        message[:300],
    )


# ============================================================
# TARGET / ENDPOINT SAFETY
# ============================================================

def target_url(target):
    """Support common target URL field names."""
    value = target.get("endpoint_url") or target.get("url") or target.get(
        "endpoint"
    )

    if not value:
        raise ValueError(
            f"Target {target.get('id', '?')} has no endpoint URL."
        )

    return str(value).strip()


def validate_endpoint(url, method="http"):
    """
    Validate endpoint syntax and reject obvious private/local targets.

    Note: hostname DNS resolution can change between validation and
    connection. For untrusted user-configured targets, enforce outbound
    network restrictions at the hosting/network layer as well.
    """
    parsed = urlparse(url)

    allowed_schemes = {"https", "wss"}
    if parsed.scheme.lower() not in allowed_schemes:
        raise ValueError(
            "Only HTTPS and secure WebSocket (WSS) endpoints are allowed."
        )

    if not parsed.hostname:
        raise ValueError("Endpoint has no hostname.")

    if parsed.username or parsed.password:
        raise ValueError(
            "Credentials embedded in endpoint URLs are not allowed."
        )

    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        raise ValueError("Endpoint port is invalid.")

    hostname = parsed.hostname.lower().rstrip(".")

    if hostname in {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
    }:
        raise ValueError("Local and metadata endpoints are not allowed.")

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None

    if address is not None:
        if not address.is_global:
            raise ValueError(
                "Private, reserved, and non-global IP addresses are blocked."
            )

    return True


def platform_slug(platform):
    """Convert a platform name into an environment-variable-safe slug."""
    return re.sub(r"[^A-Z0-9]", "_", str(platform).upper()).strip("_")


def auth_headers(target):
    """Load optional per-platform bearer tokens from environment variables."""
    platform = target.get("platform", "")
    slug = platform_slug(platform)
    token = os.getenv(f"TARGET_{slug}_TOKEN", "").strip()

    headers = {
        "Accept": "application/json",
        "User-Agent": "SolveEase-Task-Engine/2.0",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"

    return headers


# ============================================================
# TASK NORMALIZATION
# ============================================================

def safe_float(value, default=0.0):
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default


def safe_int(value, default=0):
    try:
        number = int(float(value))
        return number if number >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def normalize_tasks(data, target):
    """
    Normalize common JSON feed shapes into the task schema.

    Accepted response shapes:
        [...]
        {"tasks": [...]}
        {"data": [...]}

    Required task identifier fields:
        id, task_id, uuid, or external_id
    """
    if isinstance(data, dict):
        items = data.get("tasks")
        if items is None:
            items = data.get("data")
        if items is None:
            items = data.get("results")
        if items is None:
            items = []
    elif isinstance(data, list):
        items = data
    else:
        items = []

    if not isinstance(items, list):
        return []

    platform = str(target.get("platform", "unknown"))
    default_category = str(target.get("category", "general"))

    normalized = []

    for item in items:
        if not isinstance(item, dict):
            continue

        external_id = (
            item.get("task_id")
            or item.get("id")
            or item.get("uuid")
            or item.get("external_id")
        )

        if external_id is None:
            continue

        external_id = str(external_id).strip()
        if not external_id or len(external_id) > 250:
            continue

        payout = safe_float(
            item.get(
                "payout_usd",
                item.get("payout", item.get("reward", 0)),
            )
        )
        seconds = safe_int(
            item.get(
                "est_seconds",
                item.get(
                    "estimated_seconds",
                    item.get("duration_seconds", 0),
                ),
            )
        )

        if payout < 0 or payout > 1_000_000:
            continue

        if seconds > 31_536_000:
            continue

        category = str(item.get("category") or default_category)[:100]

        # Keep a copy of the original task data for downstream consumers.
        # Reject unserializable values instead of crashing the entire feed.
        try:
            payload = json.loads(
                json.dumps(item, ensure_ascii=False, default=str)
            )
        except (TypeError, ValueError):
            continue

        normalized.append(
            {
                "task_id": f"{platform}:{external_id}",
                "platform": platform[:100],
                "category": category,
                "payload": json.dumps(
                    payload, ensure_ascii=False, separators=(",", ":")
                ),
                "payout": payout,
                "seconds": seconds,
            }
        )

    return normalized


# ============================================================
# SOURCE FETCHERS
# ============================================================

async def fetch_http(client, target):
    """Fetch a JSON feed over HTTPS."""
    url = target_url(target)
    validate_endpoint(url, "http")

    response = await client.get(
        url,
        headers=auth_headers(target),
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code == 429:
        raise RuntimeError("Source rate-limited the request (HTTP 429).")

    response.raise_for_status()

    content_type = response.headers.get("content-type", "").lower()

    if "json" not in content_type:
        # Some public feeds omit or mislabel the content type.
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "Source response was not valid JSON."
            ) from exc
    else:
        data = response.json()

    return normalize_tasks(data, target)


async def fetch_websocket(target):
    """Read JSON messages from a secure WebSocket endpoint."""
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError(
            "WebSocket source requires the 'websockets' package."
        ) from exc

    url = target_url(target)
    validate_endpoint(url, "websocket")

    headers = auth_headers(target)
    connection_options = {
        "open_timeout": REQUEST_TIMEOUT,
        "close_timeout": 5,
        "max_size": 2 * 1024 * 1024,
    }

    # websockets changed the authentication header argument name
    # across major versions. Select the compatible option.
    try:
        connection = websockets.connect(
            url,
            additional_headers=headers,
            **connection_options,
        )
    except TypeError:
        connection = websockets.connect(
            url,
            extra_headers=headers,
            **connection_options,
        )

    collected = []

    async with connection as websocket:
        try:
            raw = await asyncio.wait_for(
                websocket.recv(),
                timeout=REQUEST_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return []

        try:
            data = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "WebSocket returned a non-JSON message."
            ) from exc

        collected.extend(normalize_tasks(data, target))

    return collected


async def fetch_playwright(target):
    """
    Open a browser page and collect JSON responses from the same host.

    Install Playwright and its Chromium browser in the deployment if
    browser targets are enabled. Browser responses may not represent
    every site's API; configure the correct source endpoint.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Browser source requires playwright and installed Chromium."
        ) from exc

    url = target_url(target)
    validate_endpoint(url, "http")
    expected_host = urlparse(url).hostname

    collected = []
    browser_errors = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)

        try:
            context = await browser.new_context(
                extra_http_headers=auth_headers(target)
            )
            page = await context.new_page()

            async def on_response(response):
                try:
                    response_url = response.url
                    parsed_response = urlparse(response_url)

                    if parsed_response.hostname != expected_host:
                        return

                    if parsed_response.scheme != "https":
                        return

                    content_type = (
                        response.headers.get("content-type", "").lower()
                    )

                    if "json" not in content_type:
                        return

                    if response.status >= 400:
                        return

                    data = await response.json()
                    collected.extend(normalize_tasks(data, target))

                except Exception as exc:
                    # A single unrelated browser response should not
                    # cancel the page navigation.
                    browser_errors.append(type(exc).__name__)

            page.on("response", on_response)

            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=REQUEST_TIMEOUT * 1000,
            )

            # Allow the page's initial asynchronous requests to finish.
            await page.wait_for_timeout(1500)

            await context.close()

        finally:
            await browser.close()

    if browser_errors:
        logger.debug(
            "Browser source %s had %s response parsing errors.",
            target.get("id"),
            len(browser_errors),
        )

    # Deduplicate within the same browser poll.
    unique = {}
    for task in collected:
        unique[task["task_id"]] = task

    return list(unique.values())


async def fetch_target(client, target):
    """Choose the fetcher from the endpoint scheme or target configuration."""
    url = target_url(target)
    parsed = urlparse(url)

    fetch_mode = str(
        target.get("fetch_mode")
        or target.get("source_type")
        or target.get("method")
        or ""
    ).strip().lower()

    if parsed.scheme.lower() == "wss" or fetch_mode in {
        "websocket",
        "ws",
        "wss",
    }:
        return await fetch_websocket(target)

    if fetch_mode in {"playwright", "browser"}:
        return await fetch_playwright(target)

    return await fetch_http(client, target)


# ============================================================
# DATABASE TARGET LOADING
# ============================================================

def load_targets(tier):
    """Load active targets in a tier whose cooldown has expired."""
    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM targets
            WHERE active = TRUE
              AND tier = %s
              AND (
                    cooldown_until IS NULL
                    OR cooldown_until <= NOW()
                  )
            ORDER BY id
            """,
            (tier,),
        ).fetchall()

    return rows


def record_event(conn, task_id, event_type, details=""):
    """Write an audit event using the existing task_events schema."""
    conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, details)
        VALUES (%s, %s, %s)
        """,
        (task_id, event_type, str(details)[:500]),
    )


# ============================================================
# TASK PERSISTENCE
# ============================================================

def save_tasks(target, tasks):
    """
    Insert normalized tasks and audit events in one transaction.

    IMPORTANT:
    roc_score is a generated PostgreSQL column. It must not appear
    in this INSERT's column list or values.
    """
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

        # An empty poll means the source returned no tasks.
        # Existing duplicate tasks still mean the source returned data.
        conn.execute(
            """
            UPDATE targets
            SET consecutive_empty_polls =
                    CASE
                        WHEN %s > 0 THEN 0
                        ELSE COALESCE(consecutive_empty_polls, 0) + 1
                    END,
                consecutive_failures = 0,
                last_checked = NOW(),
                cooldown_until = NULL
            WHERE id = %s
            """,
            (len(tasks), target["id"]),
        )

    return inserted


def record_failure(target, error):
    """Record a source failure and apply cooldown after repeated failures."""
    target_id = target.get("id")

    logger.error(
        "Target %s (%s) scan failed: %s: %s",
        target_id,
        target.get("platform", "unknown"),
        type(error).__name__,
        str(error)[:250],
    )

    try:
        with connect_db() as conn:
            conn.execute(
                """
                UPDATE targets
                SET consecutive_failures =
                        COALESCE(consecutive_failures, 0) + 1,
                    last_checked = NOW(),
                    cooldown_until =
                        CASE
                            WHEN COALESCE(consecutive_failures, 0) + 1 >= %s
                            THEN NOW() + INTERVAL '60 seconds'
                            ELSE cooldown_until
                        END
                WHERE id = %s
                """,
                (FAILURE_THRESHOLD, target_id),
            )

    except Exception as db_exc:
        database_error("Record target failure", db_exc)


# ============================================================
# TARGET POLLING AND FAILOVER
# ============================================================

async def poll_target(client, target, semaphore):
    """Poll one target without allowing its failure to stop other targets."""
    async with semaphore:
        try:
            tasks = await fetch_target(client, target)
            inserted = await asyncio.to_thread(save_tasks, target, tasks)

            logger.info(
                "Target %s (%s): received=%s inserted=%s",
                target.get("id"),
                target.get("platform", "unknown"),
                len(tasks),
                inserted,
            )

            return {
                "target_id": target.get("id"),
                "received": len(tasks),
                "inserted": inserted,
                "ok": True,
            }

        except Exception as exc:
            await asyncio.to_thread(record_failure, target, exc)

            return {
                "target_id": target.get("id"),
                "received": 0,
                "inserted": 0,
                "ok": False,
            }


def choose_tier(primary_targets):
    """
    Use primary tier while it has healthy targets.

    Fall back to tier 2 when the primary tier is empty or its targets
    have accumulated enough empty polls or failures.
    """
    if not primary_targets:
        return 2

    for target in primary_targets:
        empty_polls = safe_int(target.get("consecutive_empty_polls"))
        failures = safe_int(target.get("consecutive_failures"))

        if (
            empty_polls < EMPTY_POLL_THRESHOLD
            and failures < FAILURE_THRESHOLD
        ):
            return 1

    return 2


# ============================================================
# CONTINUOUS SCANNER
# ============================================================

async def run_smart_failover_engine():
    """Continuously scan configured targets and recover from loop errors."""
    logger.info(
        "SolveEase worker starting: poll_interval=%ss max_concurrent=%s",
        POLL_INTERVAL,
        MAX_CONCURRENT_TARGETS,
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TARGETS)

    timeout = httpx.Timeout(
        REQUEST_TIMEOUT,
        connect=min(10, REQUEST_TIMEOUT),
    )

    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT_TARGETS * 2,
        max_keepalive_connections=MAX_CONCURRENT_TARGETS,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        follow_redirects=False,
    ) as client:

        while True:
            try:
                primary_targets = await asyncio.to_thread(load_targets, 1)
                tier = choose_tier(primary_targets)

                targets = (
                    primary_targets
                    if tier == 1
                    else await asyncio.to_thread(load_targets, 2)
                )

                if not targets and tier == 2:
                    # Keep primary targets eligible for another attempt
                    # if there are no currently eligible backup targets.
                    targets = primary_targets

                if not targets:
                    logger.warning(
                        "No eligible targets found in tiers 1 or 2."
                    )
                else:
                    logger.info(
                        "Starting scan: tier=%s eligible_targets=%s",
                        tier,
                        len(targets),
                    )

                    results = await asyncio.gather(
                        *[
                            poll_target(client, target, semaphore)
                            for target in targets
                        ],
                        return_exceptions=True,
                    )

                    unexpected = sum(
                        1 for result in results
                        if isinstance(result, BaseException)
                    )

                    if unexpected:
                        logger.warning(
                            "%s unexpected target task errors occurred.",
                            unexpected,
                        )

                    successful = sum(
                        1 for result in results
                        if isinstance(result, dict) and result.get("ok")
                    )

                    logger.info(
                        "Scan complete: tier=%s targets=%s successful=%s",
                        tier,
                        len(targets),
                        successful,
                    )

            except asyncio.CancelledError:
                logger.info("Worker cancellation requested.")
                raise

            except Exception as exc:
                if isinstance(exc, psycopg.Error):
                    database_error("Scanner loop", exc)
                else:
                    logger.exception(
                        "Unexpected scanner loop error: %s",
                        type(exc).__name__,
                    )

            await asyncio.sleep(POLL_INTERVAL + random.uniform(0, 2))


if __name__ == "__main__":
    try:
        asyncio.run(run_smart_failover_engine())
    except KeyboardInterrupt:
        logger.info("SolveEase worker stopped by operator.")
