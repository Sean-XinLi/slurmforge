from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StorageSpec:
    root: str

    def root_path(self, project_root: Path) -> Path:
        path = Path(self.root)
        return path if path.is_absolute() else (project_root / path)


@dataclass(frozen=True)
class ArtifactStoreSpec:
    strategy: str = ""
    fallback_strategy: str | None = None
    verify_digest: bool = False
    fail_on_verify_error: bool = False
