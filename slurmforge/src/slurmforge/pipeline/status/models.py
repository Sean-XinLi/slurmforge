from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AttemptResult:
    train_exit_code: int | None = None
    eval_exit_code: int | None = None
    job_key: str = ""
    slurm_job_id: str = ""
    slurm_array_job_id: str = ""
    slurm_array_task_id: str = ""
    result_dir: str = ""
    log_dir: str = ""
    train_log: str | None = None
    eval_log: str | None = None
    slurm_out: str | None = None
    slurm_err: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "train_exit_code", None if self.train_exit_code is None else int(self.train_exit_code))
        object.__setattr__(self, "eval_exit_code", None if self.eval_exit_code is None else int(self.eval_exit_code))
        object.__setattr__(self, "job_key", str(self.job_key or ""))
        object.__setattr__(self, "slurm_job_id", str(self.slurm_job_id or ""))
        object.__setattr__(self, "slurm_array_job_id", str(self.slurm_array_job_id or ""))
        object.__setattr__(self, "slurm_array_task_id", str(self.slurm_array_task_id or ""))
        object.__setattr__(self, "result_dir", str(self.result_dir or ""))
        object.__setattr__(self, "log_dir", str(self.log_dir or ""))
        object.__setattr__(self, "train_log", None if self.train_log in (None, "") else str(self.train_log))
        object.__setattr__(self, "eval_log", None if self.eval_log in (None, "") else str(self.eval_log))
        object.__setattr__(self, "slurm_out", None if self.slurm_out in (None, "") else str(self.slurm_out))
        object.__setattr__(self, "slurm_err", None if self.slurm_err in (None, "") else str(self.slurm_err))


@dataclass(frozen=True)
class ExecutionStatus:
    schema_version: int = 1
    state: str = "running"
    slurm_state: str = ""
    failure_class: str | None = None
    failed_stage: str | None = None
    reason: str = ""
    train_exit_code: int | None = None
    eval_exit_code: int | None = None
    shell_exit_code: int | None = None
    job_key: str = ""
    slurm_job_id: str = ""
    slurm_array_job_id: str = ""
    slurm_array_task_id: str = ""
    started_at: str = ""
    finished_at: str = ""
    result_dir: str = ""
    train_log: str | None = None
    eval_log: str | None = None
    slurm_out: str | None = None
    slurm_err: str | None = None
