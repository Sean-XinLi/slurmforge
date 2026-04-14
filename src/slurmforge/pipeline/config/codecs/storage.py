from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any

from ....errors import ConfigContractError
from ..models.storage import (
    SqliteBackendConfig,
    SqliteOptions,
    StorageBackendConfig,
    StorageConfigSpec,
    StorageExportsConfig,
)

_VALID_ENGINES = frozenset({"none", "sqlite"})
_VALID_JOURNAL_MODES = frozenset({"DELETE", "TRUNCATE"})


def normalize_storage_config(value: Any) -> StorageConfigSpec:
    if value is None:
        return StorageConfigSpec()
    if not isinstance(value, dict):
        raise ConfigContractError("storage must be a mapping")
    backend = _normalize_backend(value.get("backend"))
    exports = _normalize_exports(value.get("exports"))
    return StorageConfigSpec(backend=backend, exports=exports)


def _normalize_backend(value: Any) -> StorageBackendConfig:
    if value is None:
        return StorageBackendConfig()
    if not isinstance(value, dict):
        raise ConfigContractError("storage.backend must be a mapping")
    engine = str(value.get("engine", "none") or "none").strip().lower()
    if engine not in _VALID_ENGINES:
        raise ConfigContractError(
            f"storage.backend.engine must be one of {sorted(_VALID_ENGINES)}, got {engine!r}"
        )
    sqlite = _normalize_sqlite_backend(value.get("sqlite"))
    return StorageBackendConfig(engine=engine, sqlite=sqlite)


def _normalize_sqlite_backend(value: Any) -> SqliteBackendConfig:
    if value is None:
        return SqliteBackendConfig()
    if not isinstance(value, dict):
        raise ConfigContractError("storage.backend.sqlite must be a mapping")
    raw_path = value.get("path", "auto")
    path = str(raw_path if raw_path is not None else "auto").strip()
    if path == "":
        raise ConfigContractError(
            "storage.backend.sqlite.path must be 'auto' or a relative path; empty string is not allowed"
        )
    if path.lower() == "auto":
        path = "auto"
    # Syntax validation only: reject absolute paths.
    # Semantic validation (must resolve within batch_root) happens at storage resolve time.
    if path != "auto" and PurePosixPath(path).is_absolute():
        raise ConfigContractError(
            f"storage.backend.sqlite.path must be 'auto' or a relative path, got absolute: {path!r}"
        )
    options = _normalize_sqlite_options(value.get("options"))
    return SqliteBackendConfig(path=path, options=options)


def _normalize_sqlite_options(value: Any) -> SqliteOptions:
    if value is None:
        return SqliteOptions()
    if not isinstance(value, dict):
        raise ConfigContractError("storage.backend.sqlite.options must be a mapping")
    busy_timeout_ms = int(value.get("busy_timeout_ms") or 5000)
    journal_mode = str(value.get("journal_mode") or "DELETE").strip().upper()
    synchronous = str(value.get("synchronous") or "NORMAL").strip().upper()
    if journal_mode not in _VALID_JOURNAL_MODES:
        raise ConfigContractError(
            f"storage.backend.sqlite.options.journal_mode must be one of "
            f"{sorted(_VALID_JOURNAL_MODES)}, got {journal_mode!r}"
        )
    return SqliteOptions(
        busy_timeout_ms=busy_timeout_ms,
        journal_mode=journal_mode,
        synchronous=synchronous,
    )


def _normalize_exports(value: Any) -> StorageExportsConfig:
    if value is None:
        return StorageExportsConfig()
    if not isinstance(value, dict):
        raise ConfigContractError("storage.exports must be a mapping")
    planning_recovery = bool(value.get("planning_recovery", True))
    return StorageExportsConfig(planning_recovery=planning_recovery)


def serialize_storage_config(config: StorageConfigSpec) -> dict[str, Any]:
    return {
        "backend": {
            "engine": config.backend.engine,
            "sqlite": {
                "path": config.backend.sqlite.path,
                "options": {
                    "busy_timeout_ms": config.backend.sqlite.options.busy_timeout_ms,
                    "journal_mode": config.backend.sqlite.options.journal_mode,
                    "synchronous": config.backend.sqlite.options.synchronous,
                },
            },
        },
        "exports": {
            "planning_recovery": config.exports.planning_recovery,
        },
    }
