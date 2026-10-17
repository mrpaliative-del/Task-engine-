
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;

-- Registered public or explicitly authorized data sources.
CREATE TABLE IF NOT EXISTS targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform_name TEXT NOT NULL UNIQUE,
    api_endpoint TEXT NOT NULL,
    extraction_method TEXT NOT NULL
        CHECK (extraction_method IN ('httpx', 'playwright', 'websocket')),
    tier INTEGER NOT NULL DEFAULT 1 CHECK (tier IN (1, 2)),
    consecutive_empty_polls INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_empty_polls >= 0),
    consecutive_failures INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_failures >= 0),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    last_checked TEXT,
    cooldown_until TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS targets_priority_idx
ON targets (is_active, tier, consecutive_failures);

-- Task queue.
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL UNIQUE,
    source_target_id INTEGER REFERENCES targets(id),
    source_platform TEXT NOT NULL,
    category TEXT NOT NULL,
    payload TEXT NOT NULL,
    payout_usd REAL NOT NULL CHECK (payout_usd >= 0),
    est_seconds INTEGER NOT NULL CHECK (est_seconds > 0),

    -- Expected payout per estimated second, not guaranteed profit.
    roc_score REAL GENERATED ALWAYS AS
        (payout_usd / est_seconds) STORED,

    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'processing', 'completed', 'failed'
        )),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,

    CHECK (attempts <= max_attempts)
);

CREATE INDEX IF NOT EXISTS tasks_roc_idx
ON tasks (status, roc_score DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS tasks_recovery_idx
ON tasks (status, locked_at);

-- Auditable task execution history.
CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES tasks(id),
    event_type TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS task_events_task_idx
ON task_events (task_id, created_at);

-- Example configuration records only.
-- Replace these reserved example URLs with real, authorized sources.
INSERT OR IGNORE INTO targets
    (platform_name, api_endpoint, extraction_method, tier, is_active)
VALUES
    ('PrimaryExample', 'https://example.com/primary-feed', 'httpx', 1, 0),
    ('BackupExample', 'https://example.com/backup-feed', 'httpx', 2, 0);
