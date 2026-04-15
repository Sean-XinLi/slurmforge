from __future__ import annotations

from ..recovery_assets import finalize_recovery_artifacts


def append_finalize_block(lines: list[str], *, planning_recovery: bool = True) -> None:
    for artifact in finalize_recovery_artifacts(planning_recovery=planning_recovery):
        lines.append(f'if [[ -n "${{{artifact.env_var}:-}}" ]]; then')
        lines.append(f'  cp "${{{artifact.env_var}}}" "${{META_DIR}}/{artifact.target_name}" || true')
        lines.append("fi")
    lines.append("")
    lines.append('printf \'%s\\n\' "[FINAL] train_status=${TRAIN_STATUS} eval_status=${EVAL_STATUS}"')
    lines.append('if [[ "${TRAIN_STATUS}" -ne 0 ]]; then')
    lines.append('  exit "${TRAIN_STATUS}"')
    lines.append("fi")
    lines.append('if [[ "${EVAL_STATUS}" -ne 0 ]]; then')
    lines.append('  exit "${EVAL_STATUS}"')
    lines.append("fi")
    lines.append('printf \'%s\\n\' "[FINAL] success result_dir=${JOB_RESULT_DIR}"')
