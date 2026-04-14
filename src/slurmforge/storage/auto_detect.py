"""Storage engine detection for existing batches.

The only authoritative source is ``meta/storage.json``. If it does not exist,
the batch is not a slurmforge-managed batch and we refuse to proceed.
"""
from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..pipeline.config.api import StorageConfigSpec
from .descriptor import read_storage_descriptor


def storage_config_for_batch(batch_root: Path) -> StorageConfigSpec:
    """Return the StorageConfigSpec for an existing batch.

    Reads ``meta/storage.json``.  Raises if the descriptor is missing.
    """
    config = read_storage_descriptor(batch_root)
    if config is not None:
        return config
    raise ConfigContractError(
        f"No storage descriptor found at {batch_root.resolve() / 'meta' / 'storage.json'}. "
        f"This batch was not created by slurmforge or predates the storage layer."
    )
