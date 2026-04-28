from __future__ import annotations

from pathlib import Path
from typing import Any

from slurmforge.outputs.records import load_stage_outputs
from slurmforge.root_model.controller_seed import write_initial_controller_state
from slurmforge.root_model.seed import (
    seed_planned_stage_statuses,
    seed_train_eval_pipeline_statuses,
)
from slurmforge.storage.batch_layout import (
    write_stage_batch_layout as write_storage_stage_batch_layout,
)
from slurmforge.storage.plan_reader import load_stage_batch_plan
from slurmforge.storage.train_eval_pipeline_layout import (
    write_train_eval_pipeline_layout as write_storage_train_eval_pipeline_layout,
)
from slurmforge.submission.generation import create_submit_generation
from slurmforge.submission.ledger import read_submission_ledger, write_submission_ledger


def write_stage_batch_layout(batch, *, spec_snapshot: dict[str, Any]) -> Path:
    batch_root = write_storage_stage_batch_layout(batch, spec_snapshot=spec_snapshot)
    seed_planned_stage_statuses(batch, batch_root)
    return batch_root


def write_train_eval_pipeline_layout(plan, *, spec_snapshot: dict[str, Any]) -> Path:
    root = write_storage_train_eval_pipeline_layout(plan, spec_snapshot=spec_snapshot)
    write_initial_controller_state(root, plan)
    seed_train_eval_pipeline_statuses(plan)
    return root

__all__ = [
    "create_submit_generation",
    "load_stage_batch_plan",
    "load_stage_outputs",
    "read_submission_ledger",
    "write_stage_batch_layout",
    "write_submission_ledger",
    "write_train_eval_pipeline_layout",
]
