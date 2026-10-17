#!/usr/bin/env python3
"""
SolveEase 300-Target Task Engine API
Neon PostgreSQL edition.

Render start command:
    uvicorn main:app --host 0.0.0.0 --port $PORT

Required environment variables:
    DATABASE_URL
    ENGINE_API_KEY
"""

import hmac
import ipaddress
import os
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator


app = FastAPI(
    title="SolveEase 300-Target Task Engine API",
    version="1.2.0",
)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
API_KEY = os.getenv("ENGINE_API_KEY", "").strip()

security = HTTPBearer(auto_error=False)

ALLOWED_METHODS = {"httpx", "playwright", "websocket"}


# --------------------------------------------------
# DATABASE ACCESS
# --------------------------------------------------

def connect_database():
    """Open a connection to Neon PostgreSQL."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")

    # Clean up any accidental wrapping quotes or whitespace from Render env vars
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
# ROOT DIAGNOSTIC
# --------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "SolveEase 300-Target Task Engine API",
        "version": "1.2.0",
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
# TARGET REGISTRATION VALIDATION
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
            raise ValueError(
                "Method must be httpx, playwright, or websocket."
            )
        return value

    @field_validator("api_endpoint")
    @classmethod
    def validate_endpoint(cls, value):
        value = value.strip()

        try:
            parsed = urlparse(value)
            hostname = parsed.hostname
            _ = parsed.port
        except ValueError as exc:
            raise ValueError("Endpoint URL is malformed.") from exc

        if parsed.scheme not in {"https", "wss"}:
            raise ValueError("Endpoint must use HTTPS or WSS.")

        if not hostname or parsed.username or parsed.password:
            raise ValueError(
                "Endpoint must have a hostname and no embedded credentials."
            )

        host = hostname.lower()

        if host in {"localhost", "localhost.localdomain"}:
            raise ValueError("Localhost targets are not permitted.")

        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None

        if address is not None and not address.is_global:
            raise ValueError(
                "Private or non-public IP addresses are not permitted."
            )

        return value


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.get("/api/health")
def health():
    """
    Verify a real database connection and table existence using robust checks.
    """
    try:
        with connect_database() as conn:
            conn.execute("SELECT 1").fetchone()

            # Check table existence securely using regclass
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
                    detail=(
                        "Database connected, but required tables "
                        "('targets', 'tasks', 'task_events') are missing."
                    ),
                )

        return {
            "status": "ok",
            "database": "connected",
            "schema": "verified",
        }

    except HTTPException:
        raise

    except Exception as exc:
        error_str = f"{type(exc).__name__}: {str(exc)}"
        print(f"Health check failed: {error_str}")
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable -> {error_str}",
        ) from exc


# --------------------------------------------------
# TASK AND QUEUE SUMMARY
# --------------------------------------------------

@app.get(
    "/api/summary",
    dependencies=[Depends(verify_api_key)],
)
def get_summary():
    result = query_db(
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
    return result[0]


# --------------------------------------------------
# TOP TASK QUEUE
# --------------------------------------------------

@app.get(
    "/api/queue/top",
    dependencies=[Depends(verify_api_key)],
)
def get_top_tasks(
    limit: int = Query(default=10, ge=1, le=100),
):
    tasks = query_db(
        """
        SELECT
            task_id,
            source_platform,
            category,
            payout_usd,
            est_seconds,
            roc_score,
            status,
            created_at
        FROM tasks
        WHERE status = 'pending'
          AND available_at <= NOW()
        ORDER BY
            roc_score DESC,
            created_at ASC,
            id ASC
        LIMIT %s
        """,
        (limit,),
    )
    return {
        "count": len(tasks),
        "top_queue": tasks,
    }


# --------------------------------------------------
# TARGET LIST
# --------------------------------------------------

@app.get(
    "/api/targets",
    dependencies=[Depends(verify_api_key)],
)
def get_targets():
    targets = query_db(
        """
        SELECT
            platform_name,
            api_endpoint,
            extraction_method,
            tier,
            is_active,
            consecutive_empty_polls,
            consecutive_failures,
            last_checked,
            cooldown_until
        FROM targets
        ORDER BY tier, platform_name
        """
    )
    return {
        "count": len(targets),
        "targets": targets,
    }


# --------------------------------------------------
# REGISTER A TARGET
# --------------------------------------------------

@app.post(
    "/api/targets/register",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
)
def register_target(target: TargetRegistration):
    parsed = urlparse(target.api_endpoint)

    expected_scheme = (
        "wss"
        if target.extraction_method == "websocket"
        else "https"
    )

    if parsed.scheme != expected_scheme:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{target.extraction_method} requires "
                f"{expected_scheme}://"
            ),
        )

    try:
        with connect_database() as conn:
            inserted = conn.execute(
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
                    target.platform_name,
                    target.api_endpoint,
                    target.extraction_method,
                    target.tier,
                ),
            ).fetchone()

            if inserted is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        "A target with this platform name "
                        "already exists."
                    ),
                )

    except HTTPException:
        raise

    except (psycopg.Error, RuntimeError) as exc:
        print(f"Target registration failed: {type(exc).__name__}")
        raise HTTPException(
            status_code=503,
            detail="Target registration failed. Check database configuration.",
        ) from exc

    return {
        "status": "registered",
        "platform_name": target.platform_name,
        "tier": target.tier,
        "is_active": False,
        "message": (
            "Registered inactive. Verify the source and its "
            "permission for automated access before activation."
        ),
    }
