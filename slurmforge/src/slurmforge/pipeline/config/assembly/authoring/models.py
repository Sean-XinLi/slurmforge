from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .....sweep import SweepSpec
from ...models import BatchSharedSpec


@dataclass(frozen=True)
class PreparedAuthoringBatchInput:
    project_root: Path
    sweep_spec: SweepSpec
    base_cfg: dict[str, Any]
    shared: BatchSharedSpec
