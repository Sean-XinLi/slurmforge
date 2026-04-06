from __future__ import annotations

from pathlib import Path

from .common import q


def append_finalize_block(lines: list[str], run_dir: Path) -> None:
    lines.append('if [[ -n "${AI_INFRA_EXECUTION_PLAN_JSON_PATH:-}" ]]; then')
    lines.append('  cp "${AI_INFRA_EXECUTION_PLAN_JSON_PATH}" "${META_DIR}/execution_plan.json" || true')
    lines.append("fi")
    lines.append(f'cp {q(str(run_dir / "resolved_config.yaml"))} "${{META_DIR}}/resolved_config.yaml" || true')
    lines.append(f'cp {q(str(run_dir / "meta" / "run_snapshot.json"))} "${{META_DIR}}/run_snapshot.json" || true')
    lines.append("")
    lines.append('printf \'%s\\n\' "[FINAL] train_status=${TRAIN_STATUS} eval_status=${EVAL_STATUS}"')
    lines.append('if [[ "${TRAIN_STATUS}" -ne 0 ]]; then')
    lines.append('  exit "${TRAIN_STATUS}"')
    lines.append("fi")
    lines.append('if [[ "${EVAL_STATUS}" -ne 0 ]]; then')
    lines.append('  exit "${EVAL_STATUS}"')
    lines.append("fi")
    lines.append('printf \'%s\\n\' "[FINAL] success result_dir=${JOB_RESULT_DIR}"')
