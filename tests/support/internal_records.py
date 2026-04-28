from __future__ import annotations

from pathlib import Path
from typing import Any

from slurmforge.materialization.stage_batch import materialize_stage_batch
from slurmforge.materialization.train_eval import materialize_train_eval_pipeline
from slurmforge.outputs.records import load_stage_outputs
from slurmforge.storage.plan_reader import load_stage_batch_plan
from slurmforge.submission.generation import create_submit_generation
from slurmforge.submission.ledger import read_submission_ledger, write_submission_ledger


def materialize_stage_batch_for_test(batch, *, spec_snapshot: dict[str, Any]) -> Path:
    return materialize_stage_batch(batch, spec_snapshot=spec_snapshot)


def materialize_train_eval_pipeline_for_test(plan, *, spec_snapshot: dict[str, Any]) -> Path:
    return materialize_train_eval_pipeline(plan, spec_snapshot=spec_snapshot)

__all__ = [
    "create_submit_generation",
    "load_stage_batch_plan",
    "load_stage_outputs",
    "read_submission_ledger",
    "materialize_stage_batch_for_test",
    "write_submission_ledger",
    "materialize_train_eval_pipeline_for_test",
]
