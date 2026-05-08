from __future__ import annotations

from dataclasses import dataclass, field

from ..io import SchemaVersion
from ..release_policy_contract import RELEASE_PER_RUN
from ..workflow_contract import WORKFLOW_PLANNED
from .workflow_state_constants import (
    DEPENDENCY_WAITING,
    DISPATCH_SUBMITTED,
    DISPATCH_SUBMITTING,
    INSTANCE_PLANNED,
)


@dataclass
class StageInstanceState:
    stage_instance_id: str
    stage_name: str
    run_id: str
    state: str = INSTANCE_PLANNED
    submission_id: str = ""
    scheduler_job_id: str = ""
    scheduler_array_task_id: str = ""
    output_ready: bool = False
    reason: str = ""
    ready_at: str = ""


@dataclass
class DependencyState:
    upstream_instance_id: str
    downstream_instance_id: str
    condition: str = "success"
    state: str = DEPENDENCY_WAITING


@dataclass
class DispatchGroupSubmissionState:
    group_id: str
    stage_instance_ids: tuple[str, ...]
    scheduler_job_id: str = ""
    array_size: int = 1
    array_throttle: int = 0
    gpus_per_task: int = 0
    task_ids_by_instance: dict[str, str] = field(default_factory=dict)
    stage_instance_gate_job_id: str = ""
    state: str = DISPATCH_SUBMITTED


@dataclass
class DispatchSubmissionState:
    submission_id: str
    stage_name: str
    role: str
    display_key: str
    instance_ids: tuple[str, ...]
    root: str
    groups: dict[str, DispatchGroupSubmissionState] = field(default_factory=dict)
    budgeted_gpus: int = 0
    state: str = DISPATCH_SUBMITTING


@dataclass
class TerminalAggregationState:
    state: str = "pending"
    workflow_terminal_state: str = ""
    reason: str = ""
    notification_control_key: str = ""
    completed_at: str = ""


@dataclass
class WorkflowState:
    pipeline_id: str
    pipeline_kind: str
    state: str = WORKFLOW_PLANNED
    current_stage: str = ""
    instances: dict[str, StageInstanceState] = field(default_factory=dict)
    dependencies: dict[str, DependencyState] = field(default_factory=dict)
    dispatch_queue: tuple[str, ...] = ()
    submissions: dict[str, DispatchSubmissionState] = field(default_factory=dict)
    terminal_aggregation: TerminalAggregationState = field(
        default_factory=TerminalAggregationState
    )
    release_policy: str = RELEASE_PER_RUN
    schema_version: int = SchemaVersion.WORKFLOW_STATE


def dependency_key(upstream_instance_id: str, downstream_instance_id: str) -> str:
    return f"{upstream_instance_id}->{downstream_instance_id}"
