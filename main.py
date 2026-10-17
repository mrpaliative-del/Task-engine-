
import hmac
import ipaddress
import os
import sqlite3
from contextlib import closing
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator


app = FastAPI(
    title="SolveEase 300-Target Task Engine API",
    version="1.1.0",
)

DB_FILE = os.getenv("DB_FILE", "task_engine.db")
API_KEY = os.getenv("ENGINE_API_KEY", "").strip()
security = HTTPBearer(auto_error=False)

ALLOWED_METHODS = {"httpx", "playwright", "websocket"}


# --------------------------------------------------
# ROOT DIAGNOSTIC
# --------------------------------------------------

@app.get("/")
def root():
    return {
        "service": "SolveEase 300-Target Task Engine API",
        "version": "1.1.0",
        "status": "running",
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
        credentials.credentials, API_KEY
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return True


# --------------------------------------------------
# DATABASE ACCESS
# --------------------------------------------------

def query_db(sql, params=()):
    if not os.path.isfile(DB_FILE):
        raise HTTPException(
            status_code=503,
            detail="Database not found. Initialize schema.sql first.",
        )

    try:
        with closing(sqlite3.connect(DB_FILE, timeout=10)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 10000")

            return [
                dict(row)
                for row in conn.execute(sql, params).fetchall()
            ]

    except sqlite3.Error as exc:
        raise HTTPException(
            status_code=503,
            detail="Database operation failed.",
        ) from exc


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
        parsed = urlparse(value)

        if parsed.scheme not in {"https", "wss"}:
            raise ValueError("Endpoint must use HTTPS or WSS.")

        if not parsed.hostname or parsed.username or parsed.password:
            raise ValueError(
                "Endpoint must have a hostname and no embedded credentials."
            )

        host = parsed.hostname.lower()

        if host in {"localhost", "localhost.localdomain"}:
            raise ValueError("Localhost targets are not permitted.")

        # Reject non-public IP literals.
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None

        if address is not None and not address.is_global:
            raise ValueError(
                "Private or non-public IPs are not permitted."
            )

        return value


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.get("/api/health")
def health():
    if not os.path.isfile(DB_FILE):
        raise HTTPException(
            status_code=503,
            detail="Database unavailable.",
        )

    return {"status": "ok"}


# --------------------------------------------------
# TASK AND QUEUE SUMMARY
# --------------------------------------------------

@app.get(
    "/api/summary",
    dependencies=[Depends(verify_api_key)],
)
def get_summary():
    result = query_db("""
        SELECT
            (SELECT COUNT(*) FROM targets) AS targets,
            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1) AS active_targets,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'pending') AS pending,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'processing') AS processing,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'completed') AS completed,
            (SELECT COUNT(*) FROM tasks
             WHERE status = 'failed') AS failed
    """)

    return result[0]


@app.get(
    "/api/queue/top",
    dependencies=[Depends(verify_api_key)],
)
def get_top_tasks(
    limit: int = Query(default=10, ge=1, le=100),
):
    tasks = query_db("""
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
          AND available_at <= CURRENT_TIMESTAMP
        ORDER BY
            roc_score DESC,
            created_at ASC,
            id ASC
        LIMIT ?
    """, (limit,))

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
    targets = query_db("""
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
    """)

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

    if not os.path.isfile(DB_FILE):
        raise HTTPException(
            status_code=503,
            detail="Database not found. Initialize schema.sql first.",
        )

    try:
        with closing(sqlite3.connect(DB_FILE, timeout=10)) as conn:
            conn.execute("PRAGMA busy_timeout = 10000")

            cursor = conn.execute("""
                INSERT OR IGNORE INTO targets (
                    platform_name,
                    api_endpoint,
                    extraction_method,
                    tier,
                    is_active
                )
                VALUES (?, ?, ?, ?, 0)
            """, (
                target.platform_name,
                target.api_endpoint,
                target.extraction_method,
                target.tier,
            ))

            conn.commit()

            if cursor.rowcount == 0:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "A target with this platform name "
                        "already exists."
                    ),
                )

    except HTTPException:
        raise

    except sqlite3.Error as exc:
        raise HTTPException(
            status_code=503,
            detail="Target registration failed.",
        ) from exc

    return {
        "status": "registered",
        "platform_name": target.platform_name,
        "tier": target.tier,
        "is_active": False,
        "message": (
            "Registered inactive. Verify the source "
            "before activation."
        ),
    }
