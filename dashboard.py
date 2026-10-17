
import os
import sqlite3
from contextlib import closing

import pandas as pd
import streamlit as st

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
DB_FILE = os.getenv("DB_FILE", "task_engine.db")

st.set_page_config(
    page_title="SolveEase Task Engine",
    page_icon="🌐",
    layout="wide",
)

st.title("🌐 SolveEase 300-Target Task Engine")
st.caption(
    "Read-only telemetry | Smart failover | Task queue | "
    "Return-on-Compute (RoC)"
)

# --------------------------------------------------
# DATABASE HELPERS
# --------------------------------------------------
def read_sql(query, params=()):
    if not os.path.isfile(DB_FILE):
        raise FileNotFoundError(
            f"Database not found: {DB_FILE}. "
            "Initialize schema.sql and check DB_FILE."
        )

    with closing(sqlite3.connect(DB_FILE, timeout=10)) as conn:
        conn.execute("PRAGMA busy_timeout = 10000")
        return pd.read_sql_query(query, conn, params=params)


def load_summary():
    result = read_sql("""
        SELECT
            (SELECT COUNT(*) FROM targets)
                AS total_targets,

            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1)
                AS active_targets,

            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1 AND tier = 1)
                AS active_tier1,

            (SELECT COUNT(*) FROM targets
             WHERE is_active = 1 AND tier = 2)
                AS active_tier2,

            (SELECT COUNT(*) FROM tasks
             WHERE status = 'pending')
                AS pending_tasks,

            (SELECT COUNT(*) FROM tasks
             WHERE status = 'processing')
                AS processing_tasks,

            (SELECT COUNT(*) FROM tasks
             WHERE status = 'completed')
                AS completed_tasks,

            (SELECT COUNT(*) FROM tasks
             WHERE status = 'failed')
                AS failed_tasks
    """)

    return result.iloc[0]


# --------------------------------------------------
# REFRESH AND DATABASE STATUS
# --------------------------------------------------
top_left, top_right = st.columns([1, 3])

with top_left:
    if st.button("🔄 Refresh telemetry", use_container_width=True):
        st.rerun()

try:
    summary = load_summary()
    with top_right:
        st.success("Database connected")

except (sqlite3.Error, FileNotFoundError, pd.errors.DatabaseError) as exc:
    st.error(f"Database unavailable: {exc}")
    st.info(
        "Confirm that schema.sql has been initialized and that "
        "DB_FILE points to the same database used by worker.py "
        "and consumer.py."
    )
    st.stop()

# --------------------------------------------------
# SYSTEM OVERVIEW
# --------------------------------------------------
st.subheader("System overview")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Registered targets", int(summary["total_targets"]))
col2.metric("Active targets", int(summary["active_targets"]))
col3.metric("Pending tasks", int(summary["pending_tasks"]))
col4.metric("Processing", int(summary["processing_tasks"]))

col5, col6, col7, col8 = st.columns(4)

col5.metric("Tier 1 active", int(summary["active_tier1"]))
col6.metric("Tier 2 active", int(summary["active_tier2"]))
col7.metric("Completed", int(summary["completed_tasks"]))
col8.metric("Failed", int(summary["failed_tasks"]))

st.divider()

# --------------------------------------------------
# TASK QUEUE
# --------------------------------------------------
st.subheader("🔥 Highest-ranked pending tasks")

st.caption(
    "RoC = estimated payout in USD divided by estimated seconds. "
    "A higher score is a ranking signal, not guaranteed income "
    "or net profit."
)

filter_col, limit_col = st.columns([2, 1])

with filter_col:
    task_status = st.selectbox(
        "Task status",
        ["pending", "processing", "completed", "failed", "all"],
        index=0,
    )

with limit_col:
    row_limit = st.selectbox(
        "Rows to display",
        [10, 25, 50, 100],
        index=1,
    )

try:
    if task_status == "all":
        task_query = """
            SELECT
                task_id,
                source_platform,
                category,
                payout_usd,
                est_seconds,
                ROUND(roc_score, 6) AS roc_usd_per_second,
                status,
                attempts,
                created_at,
                updated_at
            FROM tasks
            ORDER BY
                CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
                roc_score DESC,
                created_at ASC
            LIMIT ?
        """
        tasks = read_sql(task_query, (row_limit,))
    else:
        task_query = """
            SELECT
                task_id,
                source_platform,
                category,
                payout_usd,
                est_seconds,
                ROUND(roc_score, 6) AS roc_usd_per_second,
                status,
                attempts,
                created_at,
                updated_at
            FROM tasks
            WHERE status = ?
            ORDER BY roc_score DESC, created_at ASC
            LIMIT ?
        """
        tasks = read_sql(task_query, (task_status, row_limit))

    if tasks.empty:
        st.info(f"No {task_status} tasks found.")
    else:
        st.dataframe(
            tasks,
            use_container_width=True,
            hide_index=True,
        )

except (sqlite3.Error, pd.errors.DatabaseError) as exc:
    st.error(f"Unable to load task queue: {exc}")

# --------------------------------------------------
# TARGET HEALTH
# --------------------------------------------------
st.divider()
st.subheader("🛰️ Target health and smart failover")

try:
    targets = read_sql("""
        SELECT
            platform_name,
            tier,
            extraction_method,
            is_active,
            consecutive_empty_polls,
            consecutive_failures,
            last_checked,
            cooldown_until
        FROM targets
        ORDER BY tier ASC, platform_name ASC
    """)

    if targets.empty:
        st.info("No targets registered yet.")
    else:
        targets["status"] = targets.apply(
            lambda row: (
                "Inactive"
                if not row["is_active"]
                else (
                    "Cooling down"
                    if pd.notna(row["cooldown_until"])
                    else "Active"
                )
            ),
            axis=1,
        )

        st.dataframe(
            targets,
            use_container_width=True,
            hide_index=True,
        )

except (sqlite3.Error, pd.errors.DatabaseError) as exc:
    st.error(f"Unable to load target health: {exc}")

# --------------------------------------------------
# RECENT ACTIVITY
# --------------------------------------------------
st.divider()
st.subheader("📋 Recent task activity")

try:
    recent = read_sql("""
        SELECT
            task_id,
            source_platform,
            category,
            status,
            attempts,
            last_error,
            updated_at,
            completed_at
        FROM tasks
        ORDER BY updated_at DESC, id DESC
        LIMIT 30
    """)

    if recent.empty:
        st.info("No task activity recorded.")
    else:
        st.dataframe(
            recent,
            use_container_width=True,
            hide_index=True,
        )

except (sqlite3.Error, pd.errors.DatabaseError) as exc:
    st.error(f"Unable to load recent activity: {exc}")

# --------------------------------------------------
# FOOTER
# --------------------------------------------------
st.divider()
st.caption(
    "SolveEase Task Engine | Dashboard is read-only. "
    "Displayed task payouts and RoC scores are estimates; "
    "completed status should reflect verified provider acceptance."
)
