from __future__ import annotations

from ....errors import InternalCompilerError
from ...records import RunPlan
from ...planning.enums import LauncherKind
from .common import emit, q
from .preflight import append_stage_preflight


def append_train_block(lines: list[str], plan: RunPlan) -> None:
    train_stage = plan.train_stage
    train_workdir = str(train_stage.workdir)
    train_command = train_stage.command_text
    lines.append(f"cd {q(train_workdir)}")
    emit(lines, f"[TRAIN] workdir={train_workdir}")
    emit(lines, f"[TRAIN] command={train_command}")
    is_raw = train_stage.command_mode == "raw"
    if is_raw:
        emit(
            lines,
            "[WARN] command mode executes run.command as raw shell text; verify quoting for $, backticks, and shell expansions.",
        )
    lines.append("set +e")
    if not train_command.strip():
        raise InternalCompilerError("train_stage.command_text is empty")
    append_stage_preflight(
        lines,
        stage_label="train",
        stage_plan=plan.train_stage,
        status_var="TRAIN_STATUS",
    )
    lines.append('if [[ "${TRAIN_STATUS}" -eq 0 ]]; then')
    is_multinode_ddp = train_stage.launcher_kind == LauncherKind.DDP and int(train_stage.topology.nodes) > 1
    if is_multinode_ddp:
        lines.append('  export MASTER_PORT="${AI_INFRA_TRAIN_MASTER_PORT}"')
        lines.append('  if ! command -v scontrol >/dev/null 2>&1; then')
        lines.append('    printf \'%s\\n\' "[ERROR] multi-node DDP requires `scontrol` to resolve MASTER_ADDR"')
        lines.append("    TRAIN_STATUS=98")
        lines.append("  else")
        lines.append('    export MASTER_ADDR="$(scontrol show hostnames "${SLURM_JOB_NODELIST}" | head -n 1)"')
        lines.append('    if [[ -z "${MASTER_ADDR}" ]]; then')
        lines.append('      printf \'%s\\n\' "[ERROR] failed to resolve MASTER_ADDR from SLURM_JOB_NODELIST"')
        lines.append("      TRAIN_STATUS=98")
        lines.append("    else")
        lines.append(
            '      srun --ntasks="${AI_INFRA_TRAIN_RUNTIME_NNODES}" --ntasks-per-node=1 '
            f'bash -lc {q(train_command)} 2>&1 | tee "${{LOG_DIR}}/train.log"'
        )
        lines.append("      TRAIN_STATUS=${PIPESTATUS[0]}")
        lines.append("    fi")
        lines.append("  fi")
    elif is_raw:
        lines.append(f'  {train_command} > "${{LOG_DIR}}/train.log" 2>&1')
        lines.append("  TRAIN_STATUS=$?")
    else:
        if train_stage.launcher_kind == LauncherKind.DDP:
            lines.append('  export MASTER_PORT="${AI_INFRA_TRAIN_MASTER_PORT}"')
            lines.append('  export MASTER_ADDR="${MASTER_ADDR:-127.0.0.1}"')
        lines.append(f'  {train_command} 2>&1 | tee "${{LOG_DIR}}/train.log"')
        lines.append("  TRAIN_STATUS=${PIPESTATUS[0]}")
    lines.append("fi")
    lines.append("set -e")
    lines.append("")
