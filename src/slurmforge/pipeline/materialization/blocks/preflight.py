from __future__ import annotations

from ...planning.contracts import StageExecutionPlan
from ...planning.enums import LauncherKind, RuntimeProbe
from .common import q


def append_stage_preflight(
    lines: list[str],
    *,
    stage_label: str,
    stage_plan: StageExecutionPlan,
    status_var: str,
    indent: str = "",
) -> None:
    expected_per_node = int(stage_plan.topology.processes_per_node)
    if stage_plan.capabilities.runtime_probe == RuntimeProbe.NONE:
        return

    lines.append(f"{indent}EXPECTED_PROCESSES_PER_NODE={q(expected_per_node)}")
    lines.append(f"{indent}VISIBLE_GPU_COUNT=''")
    lines.append(f'{indent}if [[ -n "${{CUDA_VISIBLE_DEVICES:-}}" ]]; then')
    lines.append(f"{indent}  VISIBLE_GPU_COUNT=$({q(stage_plan.python_bin)} - <<'PY'")
    lines.append("import os")
    lines.append("raw = os.environ.get('CUDA_VISIBLE_DEVICES', '').strip()")
    lines.append("if not raw:")
    lines.append("    print('')")
    lines.append("else:")
    lines.append("    tokens = [item.strip() for item in raw.split(',') if item.strip()]")
    lines.append("    print(len(tokens))")
    lines.append("PY")
    lines.append(f"{indent}  )")
    lines.append(f"{indent}fi")
    lines.append(
        f'{indent}if [[ -n "${{VISIBLE_GPU_COUNT}}" ]] && [[ "${{VISIBLE_GPU_COUNT}}" -lt "${{EXPECTED_PROCESSES_PER_NODE}}" ]]; then'
    )
    lines.append(
        f'{indent}  printf \'%s\\n\' {q(f"[ERROR] {stage_label} preflight: CUDA_VISIBLE_DEVICES exposes fewer GPUs than expected processes_per_node")}'
    )
    lines.append(f"{indent}  {status_var}=97")
    lines.append(f"{indent}else")
    lines.append(
        f"{indent}  PREFLIGHT_GPU_COUNT=$({q(stage_plan.python_bin)} -c "
        + q(
            "import importlib.util; "
            "spec = importlib.util.find_spec('torch'); "
            "print(-1 if spec is None else int(__import__('torch').cuda.device_count()))"
        )
        + ' 2>/dev/null || printf "%s" "-1")'
    )
    lines.append(
        f'{indent}  if [[ "${{PREFLIGHT_GPU_COUNT}}" =~ ^[0-9]+$ ]] && [[ "${{PREFLIGHT_GPU_COUNT}}" -ge 0 ]] '
        f'&& [[ "${{PREFLIGHT_GPU_COUNT}}" -lt "${{EXPECTED_PROCESSES_PER_NODE}}" ]]; then'
    )
    lines.append(
        f'{indent}    printf \'%s\\n\' {q(f"[ERROR] {stage_label} preflight: torch.cuda.device_count() is lower than expected processes_per_node")}'
    )
    lines.append(f"{indent}    {status_var}=97")
    lines.append(f"{indent}  fi")
    lines.append(f"{indent}fi")

    if stage_plan.launcher_kind == LauncherKind.DDP and int(stage_plan.topology.nodes) > 1:
        lines.append(f'{indent}if [[ "${{{status_var}}}" -eq 0 ]] && [[ -z "${{SLURM_JOB_NODELIST:-}}" ]]; then')
        lines.append(
            f'{indent}  printf \'%s\\n\' {q(f"[ERROR] {stage_label} preflight: multi-node DDP requires SLURM_JOB_NODELIST")}'
        )
        lines.append(f"{indent}  {status_var}=97")
        lines.append(f"{indent}fi")
