from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...config.models import BatchSharedSpec
from ..models import SourceInputBatch


@dataclass(frozen=True)
class AuthoringPreparedContext:
    config_path: Path
    project_root: Path
    shared: BatchSharedSpec


@dataclass(frozen=True)
class AuthoringSourceCollection:
    batch: SourceInputBatch
    context: AuthoringPreparedContext | None = None
