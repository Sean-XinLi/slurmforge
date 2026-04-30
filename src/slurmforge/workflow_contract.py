from __future__ import annotations

TRAIN_STAGE = "train"
EVAL_STAGE = "eval"
TRAIN_EVAL_STAGE_ORDER = (TRAIN_STAGE, EVAL_STAGE)

TRAIN_EVAL_PIPELINE_KIND = "train_eval_pipeline"

STAGE_INSTANCE_GATE = "stage-instance"
DISPATCH_CATCHUP_GATE = "dispatch-catchup"
PIPELINE_GATES = (STAGE_INSTANCE_GATE, DISPATCH_CATCHUP_GATE)

BATCH_ROLE_PIPELINE_STAGE = "pipeline_stage"
BATCH_ROLE_PIPELINE_ENTRY = "pipeline_entry"
BATCH_ROLE_DISPATCH = "dispatch"

WORKFLOW_PLANNED = "planned"
WORKFLOW_STREAMING = "streaming"
WORKFLOW_FINALIZING = "finalizing"
WORKFLOW_SUCCESS = "success"
WORKFLOW_FAILED = "failed"
WORKFLOW_BLOCKED = "blocked"
WORKFLOW_TERMINAL_STATES = (
    WORKFLOW_SUCCESS,
    WORKFLOW_FAILED,
    WORKFLOW_BLOCKED,
)

__all__ = [name for name in globals() if name.isupper()]
