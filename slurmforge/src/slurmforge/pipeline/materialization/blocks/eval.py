from __future__ import annotations

from ...records import RunPlan
from ...planning.enums import LauncherKind
from .common import q
from .preflight import append_stage_preflight


def append_eval_block(lines: list[str], plan: RunPlan) -> None:
    if plan.eval_stage is None:
        return

    eval_plan = plan.eval_stage
    eval_workdir = eval_plan.workdir or "."
    lines.append('if [[ "${TRAIN_STATUS}" -ne 0 ]]; then')
    lines.append('  printf \'%s\\n\' "[EVAL] skipped because train failed train_status=${TRAIN_STATUS}"')
    lines.append('elif [[ "${EVAL_STATUS}" -ne 0 ]]; then')
    lines.append('  printf \'%s\\n\' "[EVAL] skipped because train output handoff failed eval_status=${EVAL_STATUS}"')
    lines.append("else")
    lines.append(f"  cd {q(eval_workdir)}")
    lines.append(f"  printf '%s\\n' {q(f'[EVAL] workdir={eval_workdir}')}")
    lines.append(f"  printf '%s\\n' {q(f'[EVAL] command={eval_plan.command_text}')}")
    is_raw = eval_plan.command_mode == "raw"
    if is_raw:
        lines.append(
            "  printf '%s\\n' "
            + q(
                "[WARN] command mode executes eval.command as raw shell text; verify quoting for $, backticks, and shell expansions."
            )
        )
    lines.append("  set +e")
    append_stage_preflight(
        lines,
        stage_label="eval",
        stage_plan=eval_plan,
        status_var="EVAL_STATUS",
        indent="  ",
    )
    lines.append('  if [[ "${EVAL_STATUS}" -eq 0 ]]; then')
    is_multinode_ddp = eval_plan.launcher_kind == LauncherKind.DDP and int(eval_plan.topology.nodes) > 1
    if is_multinode_ddp:
        lines.append('    export MASTER_PORT="${AI_INFRA_EVAL_MASTER_PORT}"')
        lines.append('    if ! command -v scontrol >/dev/null 2>&1; then')
        lines.append('      printf \'%s\\n\' "[ERROR] multi-node DDP eval requires `scontrol` to resolve MASTER_ADDR"')
        lines.append("      EVAL_STATUS=98")
        lines.append("    else")
        lines.append('      export MASTER_ADDR="$(scontrol show hostnames "${SLURM_JOB_NODELIST}" | head -n 1)"')
        lines.append('      if [[ -z "${MASTER_ADDR}" ]]; then')
        lines.append('        printf \'%s\\n\' "[ERROR] failed to resolve MASTER_ADDR from SLURM_JOB_NODELIST"')
        lines.append("        EVAL_STATUS=98")
        lines.append("      else")
        lines.append(
            '        srun --ntasks="${AI_INFRA_EVAL_RUNTIME_NNODES}" --ntasks-per-node=1 '
            f'bash -lc {q(eval_plan.command_text)} 2>&1 | tee "${{LOG_DIR}}/eval.log"'
        )
        lines.append("        EVAL_STATUS=${PIPESTATUS[0]}")
        lines.append("      fi")
        lines.append("    fi")
    else:
        if is_raw:
            lines.append(f'    {eval_plan.command_text} > "${{LOG_DIR}}/eval.log" 2>&1')
            lines.append("    EVAL_STATUS=$?")
        else:
            if eval_plan.launcher_kind == LauncherKind.DDP:
                lines.append('    export MASTER_PORT="${AI_INFRA_EVAL_MASTER_PORT}"')
                lines.append('    export MASTER_ADDR="${MASTER_ADDR:-127.0.0.1}"')
            lines.append(f'    {eval_plan.command_text} 2>&1 | tee "${{LOG_DIR}}/eval.log"')
            lines.append("    EVAL_STATUS=${PIPESTATUS[0]}")
    lines.append("  fi")
    lines.append("  set -e")
    lines.append("fi")
    lines.append("")
