from __future__ import annotations

from pathlib import Path

from ..root_model.runs import collect_stage_statuses
from ..status.models import TERMINAL_STATES


def batch_terminal(batch_root: Path) -> bool:
    statuses = collect_stage_statuses(batch_root)
    return bool(statuses) and all(
        status.state in TERMINAL_STATES for status in statuses
    )
