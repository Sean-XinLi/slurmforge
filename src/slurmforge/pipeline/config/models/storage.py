from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SqliteOptions:
    busy_timeout_ms: int = 5000
    journal_mode: str = "DELETE"   # "DELETE" | "TRUNCATE"  (WAL forbidden on NFS)
    synchronous: str = "NORMAL"


@dataclass(frozen=True)
class SqliteBackendConfig:
    path: str = "auto"   # "auto" → <batch_root>/meta/slurmforge.sqlite3
    options: SqliteOptions = field(default_factory=SqliteOptions)


@dataclass(frozen=True)
class StorageBackendConfig:
    engine: str = "none"   # "none" | "sqlite"
    sqlite: SqliteBackendConfig = field(default_factory=SqliteBackendConfig)


@dataclass(frozen=True)
class StorageExportsConfig:
    planning_recovery: bool = True   # sqlite mode: also export planning files for disaster recovery


@dataclass(frozen=True)
class StorageConfigSpec:
    backend: StorageBackendConfig = field(default_factory=StorageBackendConfig)
    exports: StorageExportsConfig = field(default_factory=StorageExportsConfig)
