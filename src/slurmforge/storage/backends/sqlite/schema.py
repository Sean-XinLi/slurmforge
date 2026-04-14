from __future__ import annotations

import sqlite3

# ---------------------------------------------------------------------------
# Schema version — bump when DDL changes require a migration.
# ---------------------------------------------------------------------------
SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# Planning DDL
# ---------------------------------------------------------------------------

_DDL_META = """
CREATE TABLE IF NOT EXISTS meta (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version      INTEGER NOT NULL,
    project             TEXT    NOT NULL,
    experiment_name     TEXT    NOT NULL,
    batch_name          TEXT    NOT NULL,
    batch_root          TEXT    NOT NULL,
    total_runs          INTEGER NOT NULL,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now','utc'))
);
"""

_DDL_ARRAY_GROUPS = """
CREATE TABLE IF NOT EXISTS array_groups (
    group_index         INTEGER PRIMARY KEY,
    group_signature     TEXT    NOT NULL,
    array_size          INTEGER NOT NULL,
    sbatch_path         TEXT    NOT NULL,
    records_dir         TEXT    NOT NULL,
    payload_json        TEXT    NOT NULL
);
"""

_DDL_RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT    PRIMARY KEY,
    run_index           INTEGER NOT NULL UNIQUE,
    group_index         INTEGER NOT NULL REFERENCES array_groups(group_index),
    task_index          INTEGER NOT NULL,
    run_dir_rel         TEXT    NOT NULL,
    payload_json        TEXT    NOT NULL,
    UNIQUE (group_index, task_index)
);
"""

_DDL_RUN_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS run_snapshots (
    run_id              TEXT    PRIMARY KEY REFERENCES runs(run_id),
    payload_json        TEXT    NOT NULL
);
"""

_DDL_PLANNING_DIAGNOSTICS = """
CREATE TABLE IF NOT EXISTS planning_diagnostics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT    REFERENCES runs(run_id),
    stage               TEXT    NOT NULL,
    category            TEXT    NOT NULL,
    code                TEXT    NOT NULL,
    message             TEXT    NOT NULL,
    payload_json        TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_planning_diagnostics_run_id
    ON planning_diagnostics(run_id);
"""

# ---------------------------------------------------------------------------
# Execution DDL — single wide table
# ---------------------------------------------------------------------------

_DDL_ATTEMPTS = """
CREATE TABLE IF NOT EXISTS attempts (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                  TEXT    NOT NULL REFERENCES runs(run_id),
    result_dir_rel          TEXT    NOT NULL UNIQUE,
    job_key                 TEXT,
    is_latest               INTEGER NOT NULL DEFAULT 1,
    -- execution status
    state                   TEXT,
    failure_class           TEXT,
    failed_stage            TEXT,
    reason                  TEXT,
    shell_exit_code         INTEGER,
    slurm_job_id            TEXT,
    slurm_array_job_id      TEXT,
    slurm_array_task_id     TEXT,
    started_at              TEXT,
    finished_at             TEXT,
    -- checkpoint summary
    latest_checkpoint       TEXT,
    selection_reason        TEXT,
    -- train outputs summary
    train_outputs_status    TEXT,
    primary_checkpoint      TEXT,
    -- artifact summary
    artifact_status         TEXT,
    -- canonical payloads
    status_json             TEXT,
    attempt_result_json     TEXT,
    checkpoint_json         TEXT,
    train_outputs_json      TEXT,
    artifact_json           TEXT,
    -- meta
    ingested_at             TEXT    NOT NULL DEFAULT (datetime('now','utc'))
);
CREATE INDEX IF NOT EXISTS idx_attempts_run_id ON attempts(run_id);
CREATE INDEX IF NOT EXISTS idx_attempts_latest ON attempts(run_id, is_latest);
CREATE INDEX IF NOT EXISTS idx_attempts_state ON attempts(state);
"""

_DDL_RECONCILE_STATE = """
CREATE TABLE IF NOT EXISTS reconcile_state (
    file_path           TEXT    PRIMARY KEY,
    file_mtime_ns       INTEGER NOT NULL,
    file_size           INTEGER NOT NULL,
    last_ingested_at    TEXT    NOT NULL DEFAULT (datetime('now','utc'))
);
"""

_ALL_DDL = [
    _DDL_META,
    _DDL_ARRAY_GROUPS,
    _DDL_RUNS,
    _DDL_RUN_SNAPSHOTS,
    _DDL_PLANNING_DIAGNOSTICS,
    _DDL_ATTEMPTS,
    _DDL_RECONCILE_STATE,
]


def check_schema_version(conn: sqlite3.Connection) -> None:
    """Raise if the DB schema version doesn't match the code.

    Call this on every read path to prevent silently reading from a
    half-migrated or future-version DB.
    """
    try:
        row = conn.execute("SELECT schema_version FROM meta WHERE id = 1").fetchone()
    except Exception:
        return  # meta table doesn't exist yet → fresh DB, will be initialized
    if row is None:
        return
    db_version = row["schema_version"] if isinstance(row, sqlite3.Row) else row[0]
    if db_version != SCHEMA_VERSION:
        raise RuntimeError(
            f"SQLite schema version mismatch: DB has v{db_version}, "
            f"code expects v{SCHEMA_VERSION}. Migration is required."
        )


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables (idempotent — uses IF NOT EXISTS)."""
    check_schema_version(conn)
    with conn:
        for ddl in _ALL_DDL:
            conn.executescript(ddl)
        conn.execute(
            "INSERT OR IGNORE INTO meta (id, schema_version, project, experiment_name, "
            "batch_name, batch_root, total_runs) VALUES (1, ?, '', '', '', '', 0)",
            (SCHEMA_VERSION,),
        )


def update_meta(
    conn: sqlite3.Connection,
    *,
    project: str,
    experiment_name: str,
    batch_name: str,
    batch_root: str,
    total_runs: int,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE meta SET
                project = ?, experiment_name = ?, batch_name = ?,
                batch_root = ?, total_runs = ?
            WHERE id = 1
            """,
            (project, experiment_name, batch_name, batch_root, total_runs),
        )
