from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...config_contract.defaults import (
    DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR,
    DEFAULT_ARTIFACT_STORE_STRATEGY,
    DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST,
)


@dataclass(frozen=True)
class StorageSpec:
    root: str

    def root_path(self, project_root: Path) -> Path:
        path = Path(self.root)
        return path if path.is_absolute() else (project_root / path)


@dataclass(frozen=True)
class ArtifactStoreSpec:
    strategy: str = DEFAULT_ARTIFACT_STORE_STRATEGY
    fallback_strategy: str | None = None
    verify_digest: bool = DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST
    fail_on_verify_error: bool = DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR
