from __future__ import annotations

from pathlib import Path

from ..io import write_json
from ..plans.train_eval import TrainEvalPipelinePlan
from ..workflow_contract import (
    DISPATCH_CATCHUP_GATE,
    PIPELINE_GATES,
    STAGE_INSTANCE_GATE,
)
from .sbatch_helpers import _environment_lines, _job_name, _q
from .stage_render.headers import render_control_job_headers


def _gate_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "gates"


def _gate_logs_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "logs"


def _gate_task_map_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "gates" / "task_maps"


def _safe_id(value: str) -> str:
    return value.replace("/", "_").replace(":", "_")


def _validate_gate(gate: str, *, target_id: str | None = None) -> str:
    if gate not in PIPELINE_GATES:
        raise ValueError(f"Unsupported pipeline gate: {gate}")
    if gate == STAGE_INSTANCE_GATE and not target_id:
        raise ValueError("stage-instance gate requires `target_id`")
    return gate


def _gate_file_stem(gate: str, target_id: str | None = None) -> str:
    if gate == STAGE_INSTANCE_GATE:
        return f"{_safe_id(str(target_id))}_gate"
    if gate == DISPATCH_CATCHUP_GATE:
        suffix = "" if target_id is None else f"_{_safe_id(target_id)}"
        return f"dispatch_catchup{suffix}_gate"
    return f"{gate.replace('-', '_')}_gate"


def _gate_job_parts(gate: str, target_id: str | None = None) -> tuple[str, ...]:
    if gate == STAGE_INSTANCE_GATE:
        return ("instance", _safe_id(str(target_id)), "gate")
    if gate == DISPATCH_CATCHUP_GATE:
        if target_id:
            return ("dispatch", _safe_id(target_id), "catchup")
        return ("dispatch", "catchup")
    return (gate.replace("-", "_"), str(target_id), "gate")


def render_pipeline_gate_sbatch(
    plan: TrainEvalPipelinePlan,
    gate: str,
    *,
    target_id: str | None = None,
) -> str:
    gate = _validate_gate(gate, target_id=target_id)
    control_plan = plan.control_plan
    python_bin = control_plan.runtime_plan.executor.python.bin
    stem = _gate_file_stem(gate, target_id)
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge",
            plan.pipeline_id,
            *_gate_job_parts(gate, target_id),
        ),
        stdout_path=_gate_logs_root(plan) / f"{stem}-%j.out",
        stderr_path=_gate_logs_root(plan) / f"{stem}-%j.err",
        resources=control_plan.resources,
    )
    command = (
        f'{_q(python_bin)} -m slurmforge.control.gate_runtime '
        '--pipeline-root "${PIPELINE_ROOT}"'
    )
    if gate == STAGE_INSTANCE_GATE:
        command += (
            " --event stage-instance-finished"
            f" --stage-instance-id {_q(str(target_id))}"
        )
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
    plan: TrainEvalPipelinePlan,
    gate: str,
    *,
    target_id: str | None = None,
) -> Path:
    gate = _validate_gate(gate, target_id=target_id)
    path = (
        _gate_root(plan)
        / f"{_gate_file_stem(gate, target_id)}.sbatch"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _gate_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_pipeline_gate_sbatch(
            plan,
            gate,
            target_id=target_id,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def write_stage_instance_gate_array_submit_file(
    plan: TrainEvalPipelinePlan,
    *,
    submission_id: str,
    group_id: str,
    stage_instance_ids: tuple[str, ...],
) -> Path:
    if not stage_instance_ids:
        raise ValueError("stage instance gate array requires at least one instance")
    task_map_path = (
        _gate_task_map_root(plan)
        / f"{_safe_id(submission_id)}_{_safe_id(group_id)}.json"
    )
    write_json(
        task_map_path,
        {
            "submission_id": submission_id,
            "group_id": group_id,
            "tasks": {
                str(index): stage_instance_id
                for index, stage_instance_id in enumerate(stage_instance_ids)
            },
        },
    )
    path = (
        _gate_root(plan)
        / f"{_safe_id(submission_id)}_{_safe_id(group_id)}_instance_gate.sbatch"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _gate_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_stage_instance_gate_array_sbatch(
            plan,
            submission_id=submission_id,
            group_id=group_id,
            task_map_path=task_map_path,
            array_size=len(stage_instance_ids),
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def render_stage_instance_gate_array_sbatch(
    plan: TrainEvalPipelinePlan,
    *,
    submission_id: str,
    group_id: str,
    task_map_path: Path,
    array_size: int,
) -> str:
    control_plan = plan.control_plan
    python_bin = control_plan.runtime_plan.executor.python.bin
    stem = f"{_safe_id(submission_id)}_{_safe_id(group_id)}_instance_gate"
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge", plan.pipeline_id, "instance", _safe_id(submission_id), group_id
        ),
        stdout_path=_gate_logs_root(plan) / f"{stem}-%A_%a.out",
        stderr_path=_gate_logs_root(plan) / f"{stem}-%A_%a.err",
        resources=control_plan.resources,
    )
    lines.extend(
        [
            f"#SBATCH --array=0-{array_size - 1}",
            "",
            "set -euo pipefail",
            *_environment_lines(control_plan.environment_plan),
            f"PIPELINE_ROOT={_q(plan.root_dir)}",
            f"TASK_MAP={_q(str(task_map_path))}",
            f"{_q(python_bin)} -m slurmforge.control.gate_runtime "
            '--pipeline-root "${PIPELINE_ROOT}" '
            "--event stage-instance-finished "
            '--task-map "${TASK_MAP}"',
            "",
        ]
    )
    return "\n".join(lines)


def render_pipeline_gate_barrier_sbatch(
    plan: TrainEvalPipelinePlan,
    gate: str,
    *,
    target_id: str | None = None,
    barrier_index: int,
) -> str:
    gate = _validate_gate(gate, target_id=target_id)
    stem = _gate_file_stem(gate, target_id)
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge",
            plan.pipeline_id,
            *_gate_job_parts(gate, target_id),
            "barrier",
            str(barrier_index),
        ),
        stdout_path=_gate_logs_root(plan)
        / f"{stem}-barrier-{barrier_index:03d}-%j.out",
        stderr_path=_gate_logs_root(plan)
        / f"{stem}-barrier-{barrier_index:03d}-%j.err",
        resources=plan.control_plan.resources,
    )
    target_text = "" if target_id is None else f" target={target_id}"
    lines.extend(
        [
            "set -euo pipefail",
            f'printf "%s\\n" "pipeline gate barrier gate={gate}{target_text} index={barrier_index}"',
            "",
        ]
    )
    return "\n".join(lines)


def write_pipeline_gate_barrier_file(
    plan: TrainEvalPipelinePlan,
    gate: str,
    *,
    target_id: str | None = None,
    barrier_index: int,
) -> Path:
    gate = _validate_gate(gate, target_id=target_id)
    path = (
        _gate_root(plan)
        / f"{_gate_file_stem(gate, target_id)}_barrier_{barrier_index:03d}.sbatch"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _gate_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_pipeline_gate_barrier_sbatch(
            plan,
            gate,
            target_id=target_id,
            barrier_index=barrier_index,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path
