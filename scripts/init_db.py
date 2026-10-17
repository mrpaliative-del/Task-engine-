
#!/usr/bin/env python3
"""
SolveEase 300-Target Task Engine
Neon PostgreSQL initialization and schema verification.

Usage from the project root:
    python scripts/init_db.py

Requires:
    DATABASE_URL=<your Neon PostgreSQL connection string>

This script:
- Loads schema.sql relative to the project root.
- Executes PostgreSQL schema statements in a transaction.
- Verifies required tables, columns, indexes, and foreign keys.
- Reports existing data counts.
- Never drops tables or deletes existing records.

Important:
CREATE TABLE IF NOT EXISTS does not migrate existing tables.
"""

import os
import sys
from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = PROJECT_ROOT / "schema.sql"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

REQUIRED_TABLES = {
    "targets",
    "tasks",
    "task_events",
}

REQUIRED_INDEXES = {
    "targets_priority_idx",
    "tasks_roc_idx",
    "tasks_recovery_idx",
    "tasks_available_idx",
    "task_events_task_idx",
}

REQUIRED_COLUMNS = {
    "targets": {
        "id",
        "platform_name",
        "api_endpoint",
        "extraction_method",
        "tier",
        "consecutive_empty_polls",
        "consecutive_failures",
        "is_active",
        "last_checked",
        "cooldown_until",
        "created_at",
    },
    "tasks": {
        "id",
        "task_id",
        "source_target_id",
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
        "id",
        "task_id",
        "event_type",
        "details",
        "created_at",
    },
}


def connect_database():
    """Connect securely to Neon PostgreSQL."""
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing. Configure it in your environment."
        )

    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=10,
        sslmode="require",
    )


def split_sql_statements(schema_sql: str) -> list[str]:
    """
    Split the current schema.sql into statements.

    This supports the project's ordinary DDL, indexes, and INSERT
    statements. It is not a general-purpose PostgreSQL parser and
    intentionally rejects incomplete trailing statements.
    """
    statements = []
    buffer = []

    for line in schema_sql.splitlines():
        stripped = line.strip()

        if not stripped or stripped.startswith("--"):
            continue

        buffer.append(line)

        if stripped.endswith(";"):
            statement = "\n".join(buffer).strip()
            buffer = []

            if statement:
                normalized = statement.rstrip(";").strip().upper()

                # The script manages its own transaction.
                if normalized not in {"BEGIN", "COMMIT"}:
                    statements.append(statement)

    if buffer:
        raise RuntimeError(
            "schema.sql contains an incomplete SQL statement."
        )

    if not statements:
        raise RuntimeError("schema.sql contains no SQL statements.")

    return statements


def existing_tables(conn) -> set[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_type = 'BASE TABLE'
        """
    ).fetchall()

    return {row[0] for row in rows}


def existing_indexes(conn) -> set[str]:
    rows = conn.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = current_schema()
        """
    ).fetchall()

    return {row[0] for row in rows}


def verify_schema(conn) -> None:
    """Verify tables, columns, indexes, and foreign-key constraints."""
    tables = existing_tables(conn)
    missing_tables = REQUIRED_TABLES - tables

    if missing_tables:
        raise RuntimeError(
            "Missing required tables: "
            + ", ".join(sorted(missing_tables))
        )

    indexes = existing_indexes(conn)
    missing_indexes = REQUIRED_INDEXES - indexes

    if missing_indexes:
        raise RuntimeError(
            "Missing required indexes: "
            + ", ".join(sorted(missing_indexes))
            + ". Check schema.sql and rerun initialization."
        )

    for table, expected_columns in REQUIRED_COLUMNS.items():
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            """,
            (table,),
        ).fetchall()

        actual_columns = {row[0] for row in rows}
        missing_columns = expected_columns - actual_columns

        if missing_columns:
            raise RuntimeError(
                f"Table '{table}' is missing columns: "
                + ", ".join(sorted(missing_columns))
                + ". An explicit migration may be required."
            )

    # Verify that declared foreign keys are validated by PostgreSQL.
    foreign_keys = conn.execute(
        """
        SELECT conname, convalidated
        FROM pg_constraint
        WHERE contype = 'f'
          AND connamespace = current_schema()::regnamespace
        """
    ).fetchall()

    unvalidated = [
        name for name, validated in foreign_keys if not validated
    ]

    if unvalidated:
        raise RuntimeError(
            "Unvalidated foreign-key constraints: "
            + ", ".join(unvalidated)
        )

    # Ensure the expected relationships exist.
    expected_foreign_keys = {
        ("tasks", "source_target_id", "targets"),
        ("task_events", "task_id", "tasks"),
    }

    actual_foreign_keys = conn.execute(
        """
        SELECT
            child.relname,
            child_col.attname,
            parent.relname
        FROM pg_constraint AS c
        JOIN pg_class AS child
          ON child.oid = c.conrelid
        JOIN pg_namespace AS ns
          ON ns.oid = child.relnamespace
        JOIN pg_class AS parent
          ON parent.oid = c.confrelid
        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY
          AS child_key(attnum, ord) ON TRUE
        JOIN LATERAL unnest(c.confkey) WITH ORDINALITY
          AS parent_key(attnum, ord)
          ON parent_key.ord = child_key.ord
        JOIN pg_attribute AS child_col
          ON child_col.attrelid = child.oid
         AND child_col.attnum = child_key.attnum
        WHERE c.contype = 'f'
          AND ns.nspname = current_schema()
        """
    ).fetchall()

    actual_relationships = set(actual_foreign_keys)
    missing_relationships = expected_foreign_keys - actual_relationships

    if missing_relationships:
        details = ", ".join(
            f"{child}.{column} -> {parent}"
            for child, column, parent in sorted(missing_relationships)
        )
        raise RuntimeError(
            "Missing expected foreign-key relationships: " + details
        )


def initialize_database() -> None:
    if not SCHEMA_FILE.is_file():
        raise FileNotFoundError(
            f"Cannot find PostgreSQL schema.sql at: {SCHEMA_FILE}"
        )

    schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")
    statements = split_sql_statements(schema_sql)

    print(f"Project directory: {PROJECT_ROOT}")
    print(f"Schema file:       {SCHEMA_FILE}")
    print("Database:          Neon PostgreSQL")
    print("Connection string: configured (value hidden)")
    print(f"SQL statements:    {len(statements)}")

    with connect_database() as conn:
        # Psycopg commits on successful context exit and rolls back
        # the transaction if an exception occurs.
        with conn.transaction():
            for statement in statements:
                conn.execute(statement)

            verify_schema(conn)

            target_count = conn.execute(
                "SELECT COUNT(*) FROM targets"
            ).fetchone()[0]

            task_count = conn.execute(
                "SELECT COUNT(*) FROM tasks"
            ).fetchone()[0]

            event_count = conn.execute(
                "SELECT COUNT(*) FROM task_events"
            ).fetchone()[0]

            database_name = conn.execute(
                "SELECT current_database()"
            ).fetchone()[0]

            print("\nSchema verification successful.")
            print(f"Connected database: {database_name}")
            print(f"Targets registered: {target_count}")
            print(f"Tasks stored:       {task_count}")
            print(f"Task events stored: {event_count}")
            print("Required tables:    verified")
            print("Required columns:   verified")
            print("Required indexes:   verified")
            print("Foreign keys:       verified")

    print("\nInitialization completed successfully.")
    print("Existing records were not deliberately deleted.")


if __name__ == "__main__":
    try:
        initialize_database()
    except Exception as exc:
        print(
            f"\nINITIALIZATION FAILED: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
