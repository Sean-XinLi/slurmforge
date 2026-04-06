from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelConfigSpec:
    name: str
    script: str | None = None
    yaml: str | None = None
    ddp_supported: bool | None = None
    ddp_required: bool | None = None
    estimator_profile: str | None = None
