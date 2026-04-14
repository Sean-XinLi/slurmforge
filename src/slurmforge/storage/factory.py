from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jinja2 import Environment

    from ..pipeline.config.api import StorageConfigSpec
    from .contracts import ExecutionStore, PlanningStore
    from .handle import BatchStorageHandle


def create_planning_store(
    storage_config: StorageConfigSpec,
    env: Environment,
) -> PlanningStore:
    """Create a PlanningStore for writing a new batch."""
    engine = (storage_config.backend.engine or "none").lower()
    if engine == "sqlite":
        from .backends.sqlite import SqlitePlanningStore
        return SqlitePlanningStore(env, storage_config=storage_config)
    if engine == "none":
        from .backends.filesystem import FileSystemPlanningStore
        return FileSystemPlanningStore(env)
    raise ValueError(f"Unknown storage engine: {engine!r}")


def create_planning_store_for_read(
    batch_root: Path,
    storage_config: StorageConfigSpec | None = None,
) -> PlanningStore:
    """Create a PlanningStore for reading an existing batch."""
    if storage_config is None:
        from .auto_detect import storage_config_for_batch
        storage_config = storage_config_for_batch(batch_root)

    from jinja2 import Environment
    _noop_env = Environment()

    engine = (storage_config.backend.engine or "none").lower()
    if engine == "sqlite":
        from .backends.sqlite import SqlitePlanningStore
        return SqlitePlanningStore(_noop_env, storage_config=storage_config)
    from .backends.filesystem import FileSystemPlanningStore
    return FileSystemPlanningStore(_noop_env)


def create_execution_store(
    storage_config: StorageConfigSpec,
) -> ExecutionStore:
    """Create an ExecutionStore for the given storage config."""
    engine = (storage_config.backend.engine or "none").lower()
    if engine == "sqlite":
        from .backends.sqlite import SqliteExecutionStore
        return SqliteExecutionStore(storage_config)
    from .backends.filesystem import FileSystemExecutionStore
    return FileSystemExecutionStore()


def open_batch_storage(batch_root: Path) -> BatchStorageHandle:
    """One-call entry for reading/writing against an existing batch.

    Auto-detects the storage engine from ``meta/storage.json`` (or legacy
    fallback), creates PlanningStore + ExecutionStore + ExecutionLifecycle,
    and returns a composite ``BatchStorageHandle``.
    """
    from .auto_detect import storage_config_for_batch
    from .handle import BatchStorageHandle
    from .lifecycle import ExecutionLifecycle

    storage_config = storage_config_for_batch(batch_root)
    planning = create_planning_store_for_read(batch_root, storage_config)
    execution = create_execution_store(storage_config)
    lifecycle = ExecutionLifecycle(execution)

    return BatchStorageHandle(
        batch_root=batch_root.resolve(),
        storage_config=storage_config,
        planning=planning,
        execution=execution,
        lifecycle=lifecycle,
    )
