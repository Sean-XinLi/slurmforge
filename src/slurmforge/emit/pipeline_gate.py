from __future__ import annotations

from pathlib import Path

from ..plans.train_eval import TrainEvalPipelinePlan
from ..workflow_contract import (
    EVAL_SHARD_GATE,
    FINAL_GATE,
    PIPELINE_GATES,
    TRAIN_GROUP_GATE,
)
from .sbatch_helpers import _environment_lines, _job_name, _q
from .stage_render.headers import render_control_job_headers


def _gate_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "gates"


def _gate_logs_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "logs"


def _validate_gate(gate: str, *, group_id: str | None) -> str:
    if gate not in PIPELINE_GATES:
        raise ValueError(f"Unsupported pipeline gate: {gate}")
    if gate in {TRAIN_GROUP_GATE, EVAL_SHARD_GATE} and not group_id:
        raise ValueError(f"`group_id` is required for pipeline gate `{gate}`")
    if gate == FINAL_GATE and group_id:
        raise ValueError("final pipeline gate does not accept `group_id`")
    return gate


def _gate_file_stem(gate: str, group_id: str | None) -> str:
    if gate == TRAIN_GROUP_GATE:
        return f"train_{group_id}_gate"
    if gate == EVAL_SHARD_GATE:
        return f"eval_shard_{group_id}_gate"
    return "final_gate"


def _gate_job_parts(gate: str, group_id: str | None) -> tuple[str, ...]:
    if gate == FINAL_GATE:
        return ("final", "gate")
    return (gate.replace("-", "_"), str(group_id), "gate")


def render_pipeline_gate_sbatch(
    plan: TrainEvalPipelinePlan, gate: str, *, group_id: str | None = None
) -> str:
    gate = _validate_gate(gate, group_id=group_id)
    control_plan = plan.control_plan
    python_bin = control_plan.runtime_plan.executor.python.bin
    stem = _gate_file_stem(gate, group_id)
    lines = render_control_job_headers(
        job_name=_job_name("sforge", plan.pipeline_id, *_gate_job_parts(gate, group_id)),
        stdout_path=_gate_logs_root(plan) / f"{stem}-%j.out",
        stderr_path=_gate_logs_root(plan) / f"{stem}-%j.err",
        resources=control_plan.resources,
    )
    command = (
        f'{_q(python_bin)} -m slurmforge.control.gate_runtime --pipeline-root "${{PIPELINE_ROOT}}" '
        f"--gate {_q(gate)}"
    )
    if group_id:
        command += f" --group-id {_q(group_id)}"
    lines.extend(
        [
            "set -euo pipefail",
            *_environment_lines(control_plan.environment_plan),
            f"PIPELINE_ROOT={_q(plan.root_dir)}",
            command,
            "",
        ]
    )
    return "\n".join(lines)


def write_pipeline_gate_submit_file(
    plan: TrainEvalPipelinePlan, gate: str, *, group_id: str | None = None
) -> Path:
    gate = _validate_gate(gate, group_id=group_id)
    path = _gate_root(plan) / f"{_gate_file_stem(gate, group_id)}.sbatch"
    path.parent.mkdir(parents=True, exist_ok=True)
    _gate_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_pipeline_gate_sbatch(plan, gate, group_id=group_id),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def render_pipeline_gate_barrier_sbatch(
    plan: TrainEvalPipelinePlan,
    gate: str,
    *,
    group_id: str | None = None,
    barrier_index: int,
) -> str:
    gate = _validate_gate(gate, group_id=group_id)
    stem = _gate_file_stem(gate, group_id)
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge",
            plan.pipeline_id,
            *_gate_job_parts(gate, group_id),
            "barrier",
            str(barrier_index),
        ),
        stdout_path=_gate_logs_root(plan)
        / f"{stem}-barrier-{barrier_index:03d}-%j.out",
        stderr_path=_gate_logs_root(plan)
        / f"{stem}-barrier-{barrier_index:03d}-%j.err",
        resources=plan.control_plan.resources,
    )
    group_text = "" if group_id is None else f" group={group_id}"
    lines.extend(
        [
            "set -euo pipefail",
            f'printf "%s\\n" "pipeline gate barrier gate={gate}{group_text} index={barrier_index}"',
            "",
        ]
    )
    return "\n".join(lines)


def write_pipeline_gate_barrier_file(
    plan: TrainEvalPipelinePlan,
    gate: str,
    *,
    group_id: str | None = None,
    barrier_index: int,
) -> Path:
    gate = _validate_gate(gate, group_id=group_id)
    path = (
        _gate_root(plan)
        / f"{_gate_file_stem(gate, group_id)}_barrier_{barrier_index:03d}.sbatch"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _gate_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_pipeline_gate_barrier_sbatch(
            plan,
            gate,
            group_id=group_id,
            barrier_index=barrier_index,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path
