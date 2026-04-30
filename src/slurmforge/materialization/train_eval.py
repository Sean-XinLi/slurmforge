from __future__ import annotations

from pathlib import Path
from typing import Any

from ..plans.train_eval import TrainEvalPipelinePlan
from ..root_model.seed import seed_train_eval_pipeline_statuses
from ..root_model.snapshots import refresh_train_eval_pipeline_status
from ..storage.train_eval_pipeline_layout import persist_train_eval_pipeline_layout
from ..storage.workflow import write_initial_workflow_state


def materialize_train_eval_pipeline(
    plan: TrainEvalPipelinePlan, *, spec_snapshot: dict[str, Any]
) -> Path:
    root = persist_train_eval_pipeline_layout(plan, spec_snapshot=spec_snapshot)
    write_initial_workflow_state(root, plan)
    seed_train_eval_pipeline_statuses(plan)
    refresh_train_eval_pipeline_status(root)
    return root
