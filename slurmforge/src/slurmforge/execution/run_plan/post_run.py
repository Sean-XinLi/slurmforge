from __future__ import annotations

import warnings
from pathlib import Path

from ...pipeline.checkpoints import select_checkpoint_state, write_checkpoint_state
from ...pipeline.records import RunPlan


def persist_checkpoint_state(plan: RunPlan, result_dir: Path) -> None:
    try:
        checkpoint_state = select_checkpoint_state(result_dir, tuple(plan.artifacts.checkpoint_globs))
        if checkpoint_state is None:
            return
        write_checkpoint_state(result_dir, checkpoint_state)
    except Exception as exc:
        warnings.warn(
            f"failed to persist checkpoint_state for {plan.run_id}: {exc}",
            stacklevel=2,
        )
