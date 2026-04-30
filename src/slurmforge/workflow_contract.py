from __future__ import annotations

TRAIN_STAGE = "train"
EVAL_STAGE = "eval"
TRAIN_EVAL_STAGE_ORDER = (TRAIN_STAGE, EVAL_STAGE)

TRAIN_EVAL_PIPELINE_KIND = "train_eval_pipeline"

TRAIN_GROUP_GATE = "train-group"
EVAL_SHARD_GATE = "eval-shard"
FINAL_GATE = "final"
PIPELINE_GATES = (TRAIN_GROUP_GATE, EVAL_SHARD_GATE, FINAL_GATE)

BATCH_ROLE_PIPELINE_STAGE = "pipeline_stage"
BATCH_ROLE_PIPELINE_ENTRY = "pipeline_entry"
BATCH_ROLE_EVAL_SHARD = "eval_shard"

WORKFLOW_PLANNED = "planned"
WORKFLOW_STREAMING = "streaming"
WORKFLOW_FINAL_GATE_SUBMITTED = "final_gate_submitted"
WORKFLOW_FINALIZING = "finalizing"
WORKFLOW_SUCCESS = "success"
WORKFLOW_FAILED = "failed"
WORKFLOW_BLOCKED = "blocked"
WORKFLOW_TERMINAL_STATES = (
    WORKFLOW_SUCCESS,
    WORKFLOW_FAILED,
    WORKFLOW_BLOCKED,
)

TRAIN_GROUP_TRAIN_SUBMITTED = "train_submitted"
TRAIN_GROUP_GATE_SUBMITTED = "train_group_gate_submitted"
TRAIN_GROUP_WAITING_TRAIN = "waiting_train"
TRAIN_GROUP_RECONCILED = "train_reconciled"
TRAIN_GROUP_EVAL_MATERIALIZED = "eval_materialized"
TRAIN_GROUP_EVAL_GATE_SUBMITTED = "eval_shard_gate_submitted"
TRAIN_GROUP_EVAL_MISSING = "eval_missing"
TRAIN_GROUP_WAITING_EVAL = "waiting_eval"
TRAIN_GROUP_EVAL_SKIPPED = "eval_skipped"
TRAIN_GROUP_TERMINAL = "terminal"

__all__ = [name for name in globals() if name.isupper()]
