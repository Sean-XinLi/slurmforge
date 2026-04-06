from __future__ import annotations

from pathlib import Path

from ...pipeline.materialization import build_shell_script
from ...pipeline.records import RunPlan
from ...pipeline.status import (
    begin_execution_status,
    fail_execution_status,
    finalize_execution_status,
)
from .post_run import persist_checkpoint_state
from .shell_runner import execute_script


def execute_plan(plan: RunPlan) -> int:
    run_dir = Path(plan.run_dir)
    result_dir, initial_status = begin_execution_status(run_dir)
    try:
        script = build_shell_script(plan)
        exit_code = execute_script(script)
    except Exception as exc:
        fail_execution_status(
            result_dir=result_dir,
            started_at=initial_status.started_at,
            reason=f"executor setup/launch exception: {exc}",
            shell_exit_code=1,
            failure_class="executor_error",
            failed_stage="executor",
            job_key=initial_status.job_key,
            slurm_job_id=initial_status.slurm_job_id,
            slurm_array_job_id=initial_status.slurm_array_job_id,
            slurm_array_task_id=initial_status.slurm_array_task_id,
        )
        raise
    persist_checkpoint_state(plan, result_dir)
    finalize_execution_status(
        result_dir=result_dir,
        started_at=initial_status.started_at,
        shell_exit_code=exit_code,
    )
    return exit_code
