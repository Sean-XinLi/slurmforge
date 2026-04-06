from __future__ import annotations

from pathlib import Path

from ..records import RunPlan
from .blocks.artifacts import (
    append_artifact_sync_block,
    append_attempt_result_block,
    append_slurm_log_copy_block,
)
from .blocks.env_setup import append_env_setup
from .blocks.eval import append_eval_block
from .blocks.finalize import append_finalize_block
from .blocks.preamble import append_batch_metadata, append_runtime_preamble
from .blocks.train import append_train_block
from .blocks.train_outputs import append_train_outputs_block


def build_shell_script(plan: RunPlan) -> str:
    run_dir = Path(plan.run_dir)

    lines: list[str] = []
    append_runtime_preamble(lines, run_dir)
    append_batch_metadata(lines, plan)
    append_env_setup(lines, plan)
    append_train_block(lines, plan)
    append_train_outputs_block(lines, plan)
    append_eval_block(lines, plan)
    append_artifact_sync_block(lines, plan)
    append_slurm_log_copy_block(lines)
    append_attempt_result_block(lines)
    append_finalize_block(lines, run_dir)
    return "\n".join(lines) + "\n"
