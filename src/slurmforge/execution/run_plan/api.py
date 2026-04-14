from __future__ import annotations

from pathlib import Path

from ...pipeline.materialization import build_shell_script
from ...pipeline.records import RunPlan
from ...storage import open_batch_storage
from .post_run import persist_checkpoint_state
from .shell_runner import execute_script


def execute_plan(plan: RunPlan, *, batch_root: Path) -> int:
    import os
    run_dir = Path(plan.run_dir)

    # Ensure AI_INFRA_BATCH_ROOT is available to child processes (e.g.
    # sforge-write-attempt-result).  When running under sbatch this is set by
    # the template; when the executor is invoked directly we set it here.
    _old_batch_root = os.environ.get("AI_INFRA_BATCH_ROOT")
    os.environ["AI_INFRA_BATCH_ROOT"] = str(batch_root.resolve())

    handle = open_batch_storage(batch_root)
    storage_config = handle.storage_config
    lifecycle = handle.lifecycle

    result_dir, initial_status = lifecycle.begin_attempt(run_dir)
    try:
        script = build_shell_script(plan, storage_config=storage_config)
        exit_code = execute_script(script)
    except Exception as exc:
        lifecycle.fail_attempt(
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
    lifecycle.finalize_attempt(
        result_dir=result_dir,
        started_at=initial_status.started_at,
        shell_exit_code=exit_code,
    )
    # Restore original env to avoid leaking batch_root to unrelated processes
    if _old_batch_root is None:
        os.environ.pop("AI_INFRA_BATCH_ROOT", None)
    else:
        os.environ["AI_INFRA_BATCH_ROOT"] = _old_batch_root
    return exit_code
