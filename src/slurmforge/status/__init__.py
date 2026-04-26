"""Stage status state machine API.

Public API surface:
    - StageStatusRecord, StageAttemptRecord, RunStatusRecord, PipelineStatusRecord
    - TERMINAL_STATES
    - StageRootRef, read_root_ref, write_root_ref
    - commit_stage_status, commit_attempt          # ★ sole writers
    - batched_commits                              # bulk-write compatibility helper
    - read_stage_status, list_attempts
    - state_matches
    - reconcile_stage_batch_with_slurm
    - stage_status_from_dict, attempt_from_dict

Anything not listed above is package-private and must not be imported from
outside ``slurmforge.status``.
"""
from __future__ import annotations

from .machine import (
    attempt_from_dict,
    batched_commits,
    commit_attempt,
    commit_stage_status,
    list_attempts,
    read_stage_status,
    stage_status_from_dict,
    state_matches,
)
from .models import (
    PipelineStatusRecord,
    RunStatusRecord,
    StageAttemptRecord,
    StageStatusRecord,
    TERMINAL_STATES,
)
from .reconcile import reconcile_stage_batch_with_slurm
from .root_ref import (
    StageRootRef,
    read_root_ref,
    write_root_ref,
)

__all__ = [
    "PipelineStatusRecord",
    "RunStatusRecord",
    "StageAttemptRecord",
    "StageRootRef",
    "StageStatusRecord",
    "TERMINAL_STATES",
    "attempt_from_dict",
    "batched_commits",
    "commit_attempt",
    "commit_stage_status",
    "list_attempts",
    "read_root_ref",
    "read_stage_status",
    "reconcile_stage_batch_with_slurm",
    "stage_status_from_dict",
    "state_matches",
    "write_root_ref",
]
