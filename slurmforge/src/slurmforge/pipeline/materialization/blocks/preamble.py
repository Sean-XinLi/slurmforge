from __future__ import annotations

from pathlib import Path

from ...records import RunPlan
from .common import emit, q


def append_runtime_preamble(lines: list[str], run_dir: Path) -> None:
    lines.append("set -euo pipefail")
    lines.append(f"RUN_DIR={q(str(run_dir))}")
    lines.append('JOB_KEY="${SLURM_JOB_ID:-${SLURM_ARRAY_JOB_ID:-na}_${SLURM_ARRAY_TASK_ID:-na}}"')
    lines.append('JOB_RESULT_DIR="${RUN_DIR}/job-${JOB_KEY}"')
    lines.append('LOG_DIR="${JOB_RESULT_DIR}/logs"')
    lines.append('META_DIR="${JOB_RESULT_DIR}/meta"')
    lines.append(
        'mkdir -p "$LOG_DIR" "$META_DIR" "${JOB_RESULT_DIR}/checkpoints" "${JOB_RESULT_DIR}/eval_csv" '
        '"${JOB_RESULT_DIR}/eval_images" "${JOB_RESULT_DIR}/extra"'
    )
    lines.append("")
    lines.append('printf \'%s\\n\' "[BATCH] started_at=$(date \'+%Y-%m-%d %H:%M:%S\')"')


def append_batch_metadata(lines: list[str], plan: RunPlan) -> None:
    train_stage = plan.train_stage
    emit(lines, f"[BATCH] total_jobs={plan.total_runs}")
    emit(lines, f"[BATCH] job_index={plan.run_index}")
    if plan.dispatch.array_group is not None:
        emit(lines, f"[BATCH] array_group={plan.dispatch.array_group}")
    if plan.dispatch.array_task_index is not None:
        emit(lines, f"[BATCH] array_task_index={plan.dispatch.array_task_index}")
    emit(lines, f"[BATCH] model={plan.model_name}")
    emit(lines, f"[BATCH] train_mode={plan.train_mode}")
    emit(lines, f"[BATCH] train_invocation_kind={train_stage.invocation_kind}")
    emit(lines, f"[BATCH] train_launcher_kind={train_stage.launcher_kind}")
    if train_stage.command_mode is not None:
        emit(lines, f"[BATCH] train_command_mode={train_stage.command_mode}")
    if plan.eval_stage is not None and plan.eval_stage.command_mode is not None:
        emit(lines, f"[BATCH] eval_command_mode={plan.eval_stage.command_mode}")
    emit(lines, f"[BATCH] train_recommended_total_gpus={train_stage.estimate.recommended_total_gpus}")
    emit(lines, f"[BATCH] train_estimate_reason={train_stage.estimate.reason}")
    emit(lines, f"[BATCH] run_id={plan.run_id}")
    lines.append('printf \'%s\\n\' "[BATCH] run_dir=${RUN_DIR}"')
    lines.append('printf \'%s\\n\' "[BATCH] result_dir=${JOB_RESULT_DIR}"')
    if plan.dispatch.record_path:
        emit(lines, f"[BATCH] execution_plan_path={plan.dispatch.record_path}")
    lines.append("")
