
#!/usr/bin/env python3
"""
SolveEase 300-Target Task Engine
Database initialization and schema verification.

Usage from the project root:
    python scripts/init_db.py

This script:
- Locates schema.sql relative to the project root.
- Creates task_engine.db if it does not exist.
- Applies schema.sql transactionally where possible.
- Verifies required tables and indexes.
- Never deletes an existing database.
"""

import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = PROJECT_ROOT / "schema.sql"

DB_ENV = os.getenv("DB_FILE", "task_engine.db")
DB_PATH = Path(DB_ENV)

if not DB_PATH.is_absolute():
    DB_PATH = PROJECT_ROOT / DB_PATH

REQUIRED_TABLES = {
    "targets",
    "tasks",
    "task_events",
}

REQUIRED_INDEXES = {
    "targets_priority_idx",
    "tasks_roc_idx",
    "tasks_recovery_idx",
}


def connect_database(path: Path) -> sqlite3.Connection:
    """Open SQLite with safe baseline settings."""
    conn = sqlite3.connect(
        str(path),
        timeout=15,
    )
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def existing_objects(conn: sqlite3.Connection, object_type: str) -> set:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = ?
          AND name NOT LIKE 'sqlite_%'
        """,
        (object_type,),
    ).fetchall()

    return {row[0] for row in rows}


def verify_schema(conn: sqlite3.Connection) -> None:
    """Fail clearly if essential tables or indexes are missing."""
    tables = existing_objects(conn, "table")
    missing_tables = REQUIRED_TABLES - tables

    if missing_tables:
        raise RuntimeError(
            "Missing required tables: "
            + ", ".join(sorted(missing_tables))
        )

    indexes = existing_objects(conn, "index")
    missing_indexes = REQUIRED_INDEXES - indexes

    if missing_indexes:
        raise RuntimeError(
            "Missing expected indexes: "
            + ", ".join(sorted(missing_indexes))
        )

    required_columns = {
        "targets": {
            "platform_name",
            "api_endpoint",
            "extraction_method",
            "tier",
            "consecutive_empty_polls",
            "consecutive_failures",
            "is_active",
            "last_checked",
            "cooldown_until",
        },
        "tasks": {
            "task_id",
            "source_platform",
            "category",
            "payload",
            "payout_usd",
            "est_seconds",
            "roc_score",
            "status",
            "attempts",
            "max_attempts",
            "available_at",
            "locked_at",
            "last_error",
            "created_at",
            "updated_at",
            "completed_at",
        },
        "task_events": {
            "task_id",
            "event_type",
            "details",
            "created_at",
        },
    }

    for table, expected in required_columns.items():
        actual = {
            row[1]
            for row in conn.execute(
                f"PRAGMA table_info({table})"
            ).fetchall()
        }

        missing = expected - actual

        if missing:
            raise RuntimeError(
                f"Table '{table}' is missing columns: "
                + ", ".join(sorted(missing))
                + ". An existing database may need a migration."
            )


def initialize_database() -> None:
    if not SCHEMA_FILE.is_file():
        raise FileNotFoundError(
            f"Cannot find schema.sql at: {SCHEMA_FILE}"
        )

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    database_existed = DB_PATH.exists()
    original_size = DB_PATH.stat().st_size if database_existed else 0

    print(f"Project directory: {PROJECT_ROOT}")
    print(f"Schema file:       {SCHEMA_FILE}")
    print(f"Database file:     {DB_PATH}")
    print(
        "Database status:   "
        + ("existing" if database_existed else "new")
    )

    schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")

    conn = connect_database(DB_PATH)

    try:
        # Do not use executescript() inside this explicit transaction:
        # executescript() may implicitly commit pending transactions.
        conn.execute("BEGIN")

        # Execute complete SQL statements individually so schema changes
        # can be rolled back if initialization fails.
        buffer = ""

        for line in schema_sql.splitlines():
            stripped = line.strip()

            if not stripped or stripped.startswith("--"):
                continue

            buffer += line + "\n"

            if sqlite3.complete_statement(buffer):
                statement = buffer.strip()
                buffer = ""

                if statement:
                    conn.execute(statement)

        if buffer.strip():
            raise RuntimeError(
                "schema.sql ends with an incomplete SQL statement."
            )

        conn.commit()

        verify_schema(conn)

        integrity = conn.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        if integrity != "ok":
            raise RuntimeError(
                f"SQLite integrity check failed: {integrity}"
            )

        foreign_key_errors = conn.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()

        if foreign_key_errors:
            raise RuntimeError(
                "Foreign key violations detected: "
                f"{foreign_key_errors[:5]}"
            )

        target_count = conn.execute(
            "SELECT COUNT(*) FROM targets"
        ).fetchone()[0]

        task_count = conn.execute(
            "SELECT COUNT(*) FROM tasks"
        ).fetchone()[0]

        print("\nDatabase initialization successful.")
        print(f"Targets registered: {target_count}")
        print(f"Tasks stored:       {task_count}")
        print(f"Integrity check:    {integrity}")
        print("Required schema objects verified.")

        if database_existed and original_size > 0:
            print(
                "\nExisting database detected. "
                "CREATE TABLE IF NOT EXISTS does not migrate "
                "older table definitions."
            )

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    try:
        initialize_database()
    except Exception as exc:
        print(f"\nINITIALIZATION FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
