from __future__ import annotations

from ...records import RunPlan
from .common import q


def append_train_outputs_block(lines: list[str], plan: RunPlan) -> None:
    lines.append('export AI_INFRA_TRAIN_ARTIFACT_MANIFEST="${META_DIR}/train_outputs.json"')
    lines.append('export AI_INFRA_PRIMARY_CHECKPOINT=""')
    lines.append('export AI_INFRA_BEST_CHECKPOINT=""')
    lines.append('export AI_INFRA_LATEST_CHECKPOINT=""')
    if plan.eval_stage is None:
        lines.append("")
        return

    lines.append('if [[ "${TRAIN_STATUS}" -eq 0 ]]; then')
    lines.append('  TRAIN_OUTPUT_ENV_PATH="${META_DIR}/train_outputs.env"')
    lines.append('  TRAIN_OUTPUTS_BIN="${AI_INFRA_WRITE_TRAIN_OUTPUTS_BIN:-sforge-write-train-outputs}"')
    lines.append(
        '  TRAIN_OUTPUTS_CMD=("${TRAIN_OUTPUTS_BIN}" '
        '--result_dir "${JOB_RESULT_DIR}" '
        '--manifest_path "${AI_INFRA_TRAIN_ARTIFACT_MANIFEST}" '
        '--env_path "${TRAIN_OUTPUT_ENV_PATH}" '
        f'--run_id {q(plan.run_id)} '
        f'--model_name {q(plan.model_name)} '
        f'--primary_policy {q(plan.eval_train_outputs.checkpoint_policy)})'
    )
    lines.append(f'  TRAIN_OUTPUTS_CMD+=(--workdir {q(plan.train_stage.workdir)})')
    if plan.eval_train_outputs.required:
        lines.append("  TRAIN_OUTPUTS_CMD+=(--require_primary)")
    if plan.eval_train_outputs.explicit_checkpoint:
        lines.append(
            f'  TRAIN_OUTPUTS_CMD+=(--explicit_checkpoint {q(plan.eval_train_outputs.explicit_checkpoint)})'
        )
    for pattern in plan.artifacts.checkpoint_globs:
        lines.append(f'  TRAIN_OUTPUTS_CMD+=(--checkpoint_glob {q(pattern)})')
    lines.append('  if "${TRAIN_OUTPUTS_CMD[@]}"; then')
    lines.append('    if [[ -f "${TRAIN_OUTPUT_ENV_PATH}" ]]; then')
    lines.append("      set -a")
    lines.append('      . "${TRAIN_OUTPUT_ENV_PATH}"')
    lines.append("      set +a")
    lines.append("    fi")
    lines.append("  else")
    lines.append('    printf \'%s\\n\' "[ERROR] failed to resolve train outputs for eval handoff"')
    lines.append("    EVAL_STATUS=97")
    lines.append("  fi")
    lines.append("fi")
    lines.append("")
