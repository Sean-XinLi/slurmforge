from __future__ import annotations

from ...records import RunPlan
from .common import q


def _has_artifact_patterns(plan: RunPlan) -> bool:
    return any(
        (
            plan.artifacts.checkpoint_globs,
            plan.artifacts.eval_csv_globs,
            plan.artifacts.eval_image_globs,
            plan.artifacts.extra_globs,
        )
    )


def append_artifact_sync_block(lines: list[str], plan: RunPlan) -> None:
    if not _has_artifact_patterns(plan):
        return
    lines.append('ARTIFACT_SYNC_BIN="${AI_INFRA_ARTIFACT_SYNC_BIN:-sforge-artifact-sync}"')
    lines.append('ARTIFACT_CMD=("${ARTIFACT_SYNC_BIN}" --result_dir "${JOB_RESULT_DIR}")')
    workdirs: list[str] = []
    eval_workdir = None if plan.eval_stage is None else plan.eval_stage.workdir
    for candidate in (plan.train_stage.workdir, eval_workdir):
        text = str(candidate or "").strip()
        if text and text not in workdirs:
            workdirs.append(text)
    for workdir in workdirs:
        lines.append(f"ARTIFACT_CMD+=(--workdir {q(workdir)})")
    for pattern in plan.artifacts.checkpoint_globs:
        lines.append(f"ARTIFACT_CMD+=(--checkpoint_glob {q(pattern)})")
    for pattern in plan.artifacts.eval_csv_globs:
        lines.append(f"ARTIFACT_CMD+=(--eval_csv_glob {q(pattern)})")
    for pattern in plan.artifacts.eval_image_globs:
        lines.append(f"ARTIFACT_CMD+=(--eval_image_glob {q(pattern)})")
    for pattern in plan.artifacts.extra_globs:
        lines.append(f"ARTIFACT_CMD+=(--extra_glob {q(pattern)})")
    lines.append('"${ARTIFACT_CMD[@]}" || true')
    lines.append("")


def append_slurm_log_copy_block(lines: list[str]) -> None:
    lines.append('if [[ -n "${AI_INFRA_ARRAY_LOG_DIR:-}" && -n "${SLURM_JOB_ID:-}" ]]; then')
    lines.append('  ARRAY_OUT="${AI_INFRA_ARRAY_LOG_DIR}/slurm-${SLURM_JOB_ID}.out"')
    lines.append('  ARRAY_ERR="${AI_INFRA_ARRAY_LOG_DIR}/slurm-${SLURM_JOB_ID}.err"')
    lines.append('  [[ -f "$ARRAY_OUT" ]] && cp "$ARRAY_OUT" "${LOG_DIR}/" || true')
    lines.append('  [[ -f "$ARRAY_ERR" ]] && cp "$ARRAY_ERR" "${LOG_DIR}/" || true')
    lines.append('elif [[ -n "${SLURM_JOB_ID:-}" ]]; then')
    lines.append('  LOCAL_OUT="${RUN_DIR}/slurm-${SLURM_JOB_ID}.out"')
    lines.append('  LOCAL_ERR="${RUN_DIR}/slurm-${SLURM_JOB_ID}.err"')
    lines.append('  [[ -f "$LOCAL_OUT" ]] && cp "$LOCAL_OUT" "${LOG_DIR}/" || true')
    lines.append('  [[ -f "$LOCAL_ERR" ]] && cp "$LOCAL_ERR" "${LOG_DIR}/" || true')
    lines.append("fi")
    lines.append("")


def append_attempt_result_block(lines: list[str]) -> None:
    lines.append('ATTEMPT_RESULT_BIN="${AI_INFRA_WRITE_ATTEMPT_RESULT_BIN:-sforge-write-attempt-result}"')
    lines.append(
        '"${ATTEMPT_RESULT_BIN}" '
        '--result_dir "${JOB_RESULT_DIR}" '
        '--train_exit_code "${TRAIN_STATUS}" '
        '--eval_exit_code "${EVAL_STATUS}" '
        '|| printf \'%s\\n\' "[WARN] sforge-write-attempt-result failed, exit codes may be lost"'
    )
    lines.append("")
