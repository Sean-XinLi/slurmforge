from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ....errors import ConfigContractError
from ....pipeline.config.api import StorageConfigSpec

_SQLITE_DB_FILENAME = "slurmforge.sqlite3"


def db_path_for_batch(batch_root: Path, config: StorageConfigSpec) -> Path:
    """Return the SQLite DB path for a batch.

    If ``config.backend.sqlite.path`` is ``"auto"``, the canonical location is
    ``<batch_root>/meta/slurmforge.sqlite3``. Otherwise the configured relative
    path is resolved under ``batch_root`` and must stay within it.
    """
    resolved_root = batch_root.resolve()
    configured = str(config.backend.sqlite.path or "").strip()
    if configured.lower() == "auto":
        return resolved_root / "meta" / _SQLITE_DB_FILENAME
    if configured == "":
        raise ConfigContractError(
            "storage.backend.sqlite.path must be 'auto' or a relative path; empty string is not allowed"
        )

    candidate = (resolved_root / configured).resolve()
    try:
        candidate.relative_to(resolved_root)
    except ValueError:
        raise ConfigContractError(
            f"storage.backend.sqlite.path must resolve within batch_root, "
            f"but {configured!r} resolves to {candidate} which is outside {resolved_root}"
        )
    return candidate


@contextmanager
def open_batch_db(
    db_path: Path,
    *,
    config: StorageConfigSpec,
    create: bool = False,
) -> Iterator[sqlite3.Connection]:
    """Open (and optionally create) a per-batch SQLite connection.

    WAL mode is intentionally NOT used here — per the architecture design,
    compute nodes write files over NFS and the DB is only written from a single
    coordinating process, so the extra concurrency of WAL is unnecessary and
    WAL is unreliable on NFS.

    The ``journal_mode`` is controlled by ``config.backend.sqlite.options``
    (valid values: DELETE, TRUNCATE — both are local-file-safe).
    """
    opts = config.backend.sqlite.options
    if not create and not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=opts.busy_timeout_ms / 1000)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA journal_mode = {opts.journal_mode}")
        conn.execute(f"PRAGMA synchronous = {opts.synchronous}")
        if not create:
            from .schema import check_schema_version
            check_schema_version(conn)
        yield conn
    finally:
        conn.close()
