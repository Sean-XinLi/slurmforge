from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..emit.pipeline_gate import write_pipeline_gate_submit_file
from ..plans.stage import StageBatchPlan
from ..storage.plan_reader import load_stage_batch_plan
from ..submission.generation import prepare_stage_submission
from ..submission.models import PreparedSubmission
from ..workflow_contract import TRAIN_GROUP_GATE, TRAIN_STAGE


@dataclass(frozen=True)
class PreparedInitialPipeline:
    train_batch: StageBatchPlan
    train_submission: PreparedSubmission


def prepare_initial_pipeline_files(plan) -> PreparedInitialPipeline:
    train_batch = load_stage_batch_plan(
        Path(plan.stage_batches[TRAIN_STAGE].submission_root)
    )
    train_submission = prepare_stage_submission(train_batch)
    for group in train_batch.group_plans:
        write_pipeline_gate_submit_file(
            plan,
            TRAIN_GROUP_GATE,
            group_id=group.group_id,
        )
    return PreparedInitialPipeline(
        train_batch=train_batch,
        train_submission=train_submission,
    )
