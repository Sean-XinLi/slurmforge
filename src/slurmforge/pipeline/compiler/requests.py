from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Union


@dataclass(frozen=True)
class AuthoringSourceRequest:
    config_path: Path
    cli_overrides: tuple[str, ...] = field(default_factory=tuple)
    project_root: Path | None = None
    default_batch_name: str | None = None
    manifest_extras: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "config_path", Path(self.config_path))
        object.__setattr__(self, "cli_overrides", tuple(str(item) for item in self.cli_overrides))
        object.__setattr__(self, "project_root", None if self.project_root is None else Path(self.project_root))
        object.__setattr__(self, "manifest_extras", copy.deepcopy(self.manifest_extras or {}))


@dataclass(frozen=True)
class ReplaySourceRequest:
    source_run_dir: Path | None = None
    source_batch_root: Path | None = None
    run_ids: tuple[str, ...] = field(default_factory=tuple)
    run_indices: tuple[int, ...] = field(default_factory=tuple)
    cli_overrides: tuple[str, ...] = field(default_factory=tuple)
    project_root: Path | None = None
    default_batch_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_run_dir", None if self.source_run_dir is None else Path(self.source_run_dir))
        object.__setattr__(
            self,
            "source_batch_root",
            None if self.source_batch_root is None else Path(self.source_batch_root),
        )
        object.__setattr__(self, "run_ids", tuple(str(item) for item in self.run_ids))
        object.__setattr__(self, "run_indices", tuple(int(item) for item in self.run_indices))
        object.__setattr__(self, "cli_overrides", tuple(str(item) for item in self.cli_overrides))
        object.__setattr__(self, "project_root", None if self.project_root is None else Path(self.project_root))
        object.__setattr__(
            self,
            "default_batch_name",
            None if self.default_batch_name is None else str(self.default_batch_name),
        )


@dataclass(frozen=True)
class RetrySourceRequest:
    source_batch_root: Path
    status_query: str
    cli_overrides: tuple[str, ...] = field(default_factory=tuple)
    project_root: Path | None = None
    default_batch_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_batch_root", Path(self.source_batch_root))
        object.__setattr__(self, "status_query", str(self.status_query))
        object.__setattr__(self, "cli_overrides", tuple(str(item) for item in self.cli_overrides))
        object.__setattr__(self, "project_root", None if self.project_root is None else Path(self.project_root))
        object.__setattr__(
            self,
            "default_batch_name",
            None if self.default_batch_name is None else str(self.default_batch_name),
        )


SourceRequest = Union[AuthoringSourceRequest, ReplaySourceRequest, RetrySourceRequest]


__all__ = [
    "AuthoringSourceRequest",
    "ReplaySourceRequest",
    "RetrySourceRequest",
    "SourceRequest",
]
