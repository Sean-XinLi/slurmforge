from __future__ import annotations

from dataclasses import dataclass


SLURM_SUCCESS_STATES = {"COMPLETED"}
SLURM_FAILURE_STATES = {
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "TIMEOUT",
    "BOOT_FAIL",
    "DEADLINE",
}
SLURM_CANCELLED_STATES = {"CANCELLED", "CANCELLED+", "PREEMPTED"}
SLURM_RUNNING_STATES = {"RUNNING", "COMPLETING"}
SLURM_QUEUED_STATES = {"PENDING", "CONFIGURING", "RESIZING", "SUSPENDED"}
SLURM_TERMINAL_STATES = (
    SLURM_SUCCESS_STATES | SLURM_FAILURE_STATES | SLURM_CANCELLED_STATES
)


@dataclass(frozen=True)
class SlurmSubmitOptions:
    dependency: str = ""
    mail_user: str = ""
    mail_type: str = ""


@dataclass(frozen=True)
class SlurmJobState:
    job_id: str
    state: str
    exit_code: str = ""
    reason: str = ""
    array_job_id: str | None = None
    array_task_id: int | None = None
    job_id_raw: str = ""

    @property
    def is_terminal(self) -> bool:
        return self.state in SLURM_TERMINAL_STATES

    @property
    def is_success(self) -> bool:
        return self.state in SLURM_SUCCESS_STATES


def normalize_slurm_state(value: str) -> str:
    state = value.strip().upper()
    if " " in state:
        state = state.split(" ", 1)[0]
    if "+" in state and state != "CANCELLED+":
        state = state.split("+", 1)[0]
    return state


def failure_class_for_slurm_state(state: str) -> str | None:
    normalized = normalize_slurm_state(state)
    if normalized in SLURM_SUCCESS_STATES:
        return None
    if normalized == "OUT_OF_MEMORY":
        return "oom"
    if normalized == "TIMEOUT":
        return "timeout"
    if normalized in SLURM_CANCELLED_STATES:
        return "cancelled"
    if normalized in SLURM_FAILURE_STATES:
        return "scheduler_failure"
    return None


def stage_state_for_slurm_state(state: str) -> str | None:
    normalized = normalize_slurm_state(state)
    if normalized in SLURM_SUCCESS_STATES:
        return "success"
    if normalized in SLURM_CANCELLED_STATES:
        return "cancelled"
    if normalized in SLURM_FAILURE_STATES:
        return "failed"
    if normalized in SLURM_RUNNING_STATES:
        return "running"
    if normalized in SLURM_QUEUED_STATES:
        return "queued"
    return None
