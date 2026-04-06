from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...errors import ConfigContractError


@dataclass(frozen=True)
class CheckpointState:
    schema_version: int = 1
    latest_checkpoint_rel: str = ""
    selection_reason: str = ""
    global_step: int | None = None

    def __post_init__(self) -> None:
        latest_checkpoint_rel = str(self.latest_checkpoint_rel or "").strip()
        selection_reason = str(self.selection_reason or "").strip()
        if not latest_checkpoint_rel:
            raise ConfigContractError("CheckpointState.latest_checkpoint_rel must be a non-empty string")
        if Path(latest_checkpoint_rel).is_absolute():
            raise ConfigContractError("CheckpointState.latest_checkpoint_rel must be result_dir-relative")
        if not selection_reason:
            raise ConfigContractError("CheckpointState.selection_reason must be a non-empty string")
        object.__setattr__(self, "latest_checkpoint_rel", latest_checkpoint_rel)
        object.__setattr__(self, "selection_reason", selection_reason)
