from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..plans.train_eval import TrainEvalPipelinePlan
from ..plans.stage import StageBatchPlan
from ..storage.plan_reader import load_stage_batch_plan
from ..submission.generation import prepare_stage_submission
from ..submission.models import PreparedSubmission
from ..workflow_contract import TRAIN_STAGE


@dataclass(frozen=True)
class PreparedInitialPipeline:
    train_batch: StageBatchPlan
    train_submission: PreparedSubmission


def prepare_initial_pipeline_files(
    plan: TrainEvalPipelinePlan,
) -> PreparedInitialPipeline:
    train_batch = load_stage_batch_plan(
        Path(plan.stage_batches[TRAIN_STAGE].submission_root)
    )
    train_submission = prepare_stage_submission(train_batch)
    return PreparedInitialPipeline(
        train_batch=train_batch,
        train_submission=train_submission,
    )
