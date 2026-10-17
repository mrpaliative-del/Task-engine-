
#!/usr/bin/env python3
"""
SolveEase 300-Target Task Engine
Stale-task recovery utility.

Examples:
    # Inspect stale tasks without changing anything
    python scripts/recover_tasks.py

    # Apply safe recovery actions; uncertain tasks remain untouched
    python scripts/recover_tasks.py --apply

    # Requeue stale tasks even when submission outcome is uncertain
    # ONLY use if provider-side idempotency/reconciliation is handled.
    python scripts/recover_tasks.py --apply --retry-uncertain

    # Change the stale-task lease timeout
    python scripts/recover_tasks.py --lease-seconds 900

Important:
A stale processing task may already have been accepted by its provider.
Requeueing such a task can cause duplicate submissions.
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DB_ENV = os.getenv("DB_FILE", "task_engine.db")
DB_PATH = Path(DB_ENV)

if not DB_PATH.is_absolute():
    DB_PATH = PROJECT_ROOT / DB_PATH


def connect_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def verify_schema(conn):
    required = {
        "tasks": {
            "id",
            "task_id",
            "status",
            "attempts",
            "max_attempts",
            "locked_at",
            "available_at",
            "last_error",
            "updated_at",
        },
        "task_events": {
            "task_id",
            "event_type",
            "details",
            "created_at",
        },
    }

    for table, expected_columns in required.items():
        rows = conn.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()

        if not rows:
            raise RuntimeError(
                f"Required table '{table}' does not exist."
            )

        actual = {row["name"] for row in rows}
        missing = expected_columns - actual

        if missing:
            raise RuntimeError(
                f"Table '{table}' is missing columns: "
                + ", ".join(sorted(missing))
            )


def find_stale_tasks(conn, lease_seconds):
    # A task is stale only when it has a lock timestamp older than
    # the configured lease. NULL locks are deliberately not guessed.
    modifier = f"-{lease_seconds} seconds"

    return conn.execute(
        """
        SELECT
            id,
            task_id,
            source_platform,
            status,
            attempts,
            max_attempts,
            locked_at,
            last_error
        FROM tasks
        WHERE status = 'processing'
          AND locked_at IS NOT NULL
          AND locked_at <= datetime('now', ?)
        ORDER BY locked_at ASC, id ASC
        """,
        (modifier,),
    ).fetchall()


def record_event(conn, task_db_id, event_type, details):
    conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, details)
        VALUES (?, ?, ?)
        """,
        (task_db_id, event_type, details[:1000]),
    )


def recover(args):
    if not DB_PATH.is_file():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}"
        )

    conn = connect_db()

    try:
        verify_schema(conn)

        stale_tasks = find_stale_tasks(
            conn,
            args.lease_seconds,
        )

        print(f"Database: {DB_PATH}")
        print(f"Lease timeout: {args.lease_seconds} seconds")
        print(f"Stale processing tasks: {len(stale_tasks)}")
        print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}")

        if not stale_tasks:
            print("No stale tasks found.")
            return

        recovered = 0
        marked_failed = 0
        manual_review = 0

        if args.apply:
            conn.execute("BEGIN IMMEDIATE")

        for task in stale_tasks:
            task_id = task["id"]
            attempts = task["attempts"]
            max_attempts = task["max_attempts"]

            print(
                f"\nTask: {task['task_id']} | "
                f"Platform: {task['source_platform']} | "
                f"Attempts: {attempts}/{max_attempts} | "
                f"Locked: {task['locked_at']}"
            )

            if not args.apply:
                if attempts < max_attempts:
                    print("  DRY RUN: eligible for recovery review.")
                else:
                    print("  DRY RUN: attempt limit reached.")
                continue

            # Exhausted tasks cannot be retried automatically.
            if attempts >= max_attempts:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'failed',
                        locked_at = NULL,
                        updated_at = CURRENT_TIMESTAMP,
                        last_error = ?
                    WHERE id = ? AND status = 'processing'
                    """,
                    (
                        "Stale processing lease; maximum attempts "
                        "reached. Verify provider outcome manually.",
                        task_id,
                    ),
                )

                record_event(
                    conn,
                    task_id,
                    "recovery_failed",
                    "Stale task exceeded maximum attempts; "
                    "provider outcome requires reconciliation.",
                )
                marked_failed += 1
                print("  ACTION: marked failed; verify provider outcome.")
                continue

            # Requeueing a task whose previous outcome is unknown
            # can duplicate work. Require an explicit opt-in.
            if not args.retry_uncertain:
                record_event(
                    conn,
                    task_id,
                    "recovery_review_required",
                    "Stale processing task left unchanged because "
                    "provider submission outcome is uncertain.",
                )
                manual_review += 1
                print(
                    "  ACTION: left processing; manual reconciliation "
                    "required. Use --retry-uncertain only if safe."
                )
                continue

            cursor = conn.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    locked_at = NULL,
                    available_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP,
                    last_error = ?
                WHERE id = ? AND status = 'processing'
                """,
                (
                    "Requeued after stale lease; check provider "
                    "idempotency and prior submission outcome.",
                    task_id,
                ),
            )

            if cursor.rowcount == 1:
                record_event(
                    conn,
                    task_id,
                    "recovered_and_requeued",
                    "Requeued after stale lease with explicit "
                    "retry-uncertain authorization.",
                )
                recovered += 1
                print("  ACTION: requeued as pending.")

        if args.apply:
            conn.commit()

        print("\nRecovery summary")
        print(f"Requeued: {recovered}")
        print(f"Marked failed: {marked_failed}")
        print(f"Manual review required: {manual_review}")

        if not args.apply:
            print(
                "\nDry run only. No task statuses were changed. "
                "Use --apply to apply recovery actions."
            )

    except Exception:
        if args.apply:
            conn.rollback()
        raise
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Inspect and recover stale processing tasks."
    )

    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=900,
        help="Seconds after which a processing lock is stale (default: 900).",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply recovery actions; without this flag, run a dry run.",
    )

    parser.add_argument(
        "--retry-uncertain",
        action="store_true",
        help=(
            "Requeue stale tasks despite uncertain provider outcomes. "
            "Use only with provider idempotency or reconciliation."
        ),
    )

    args = parser.parse_args()

    if args.lease_seconds < 1:
        parser.error("--lease-seconds must be greater than zero.")

    if args.retry_uncertain and not args.apply:
        parser.error("--retry-uncertain requires --apply.")

    return args


if __name__ == "__main__":
    try:
        recover(parse_args())
    except (sqlite3.Error, OSError, RuntimeError) as exc:
        print(f"RECOVERY ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
