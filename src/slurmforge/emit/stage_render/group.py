from __future__ import annotations

from pathlib import Path

from ...io import stable_json
from ...plans.runtime import EnvironmentPlan, RuntimePlan
from ...plans.stage import GroupPlan, StageBatchPlan, StageInstancePlan
from ..sbatch_helpers import _environment_lines, _job_name, _q


def _submit_root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root) / "submit"


def _instances_by_id(batch: StageBatchPlan) -> dict[str, StageInstancePlan]:
    return {instance.stage_instance_id: instance for instance in batch.stage_instances}


def _runtime_for_group(batch: StageBatchPlan, group: GroupPlan) -> RuntimePlan:
    instances = _instances_by_id(batch)
    plans = {
        stable_json(instances[stage_instance_id].runtime_plan): instances[
            stage_instance_id
        ].runtime_plan
        for stage_instance_id in group.stage_instance_ids
    }
    if len(plans) != 1:
        raise ValueError(f"group {group.group_id} mixes multiple runtime plans")
    return next(iter(plans.values()))


def _environment_for_group(batch: StageBatchPlan, group: GroupPlan) -> EnvironmentPlan:
    instances = _instances_by_id(batch)
    plans = {
        stable_json(instances[stage_instance_id].environment_plan): instances[
            stage_instance_id
        ].environment_plan
        for stage_instance_id in group.stage_instance_ids
    }
    if len(plans) != 1:
        raise ValueError(f"group {group.group_id} mixes multiple environment plans")
    return next(iter(plans.values()))


def render_stage_group_sbatch(
    batch: StageBatchPlan, group: GroupPlan, *, generation_id: str | None = None
) -> str:
    resources = group.resources
    runtime_plan = _runtime_for_group(batch, group)
    environment_plan = _environment_for_group(batch, group)
    executor_plan = runtime_plan.executor
    python_bin = executor_plan.python.bin
    executor_module = executor_plan.module
    submit_dir = _submit_root(batch)
    log_dir = submit_dir / "logs" / (generation_id or "manual")
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={_job_name('sforge', batch.project, batch.stage_name, group.group_id)}",
        f"#SBATCH --output={_q(str(log_dir / (group.group_id + '-%A_%a.out')))}",
        f"#SBATCH --error={_q(str(log_dir / (group.group_id + '-%A_%a.err')))}",
    ]
    if resources.partition:
        lines.append(f"#SBATCH --partition={resources.partition}")
    if resources.account:
        lines.append(f"#SBATCH --account={resources.account}")
    if resources.qos:
        lines.append(f"#SBATCH --qos={resources.qos}")
    if resources.time_limit:
        lines.append(f"#SBATCH --time={resources.time_limit}")
    lines.extend(
        [
            f"#SBATCH --nodes={int(resources.nodes or 1)}",
            "#SBATCH --ntasks-per-node=1",
            f"#SBATCH --cpus-per-task={int(resources.cpus_per_task or 1)}",
        ]
    )
    if int(resources.gpus_per_node or 0) > 0:
        lines.append(f"#SBATCH --gres=gpu:{int(resources.gpus_per_node or 0)}")
    if resources.mem:
        lines.append(f"#SBATCH --mem={resources.mem}")
    if resources.constraint:
        lines.append(f"#SBATCH --constraint={resources.constraint}")
    for arg in resources.extra_sbatch_args:
        lines.append(f"#SBATCH {arg}")
    throttle = ""
    if (
        group.array_throttle is not None
        and group.array_throttle > 0
        and group.array_throttle < group.array_size
    ):
        throttle = f"%{group.array_throttle}"
    lines.extend(
        [
            f"#SBATCH --array=0-{group.array_size - 1}{throttle}",
            "",
            "set -euo pipefail",
            *_environment_lines(environment_plan),
            f"BATCH_ROOT={_q(batch.submission_root)}",
            f"GROUP_INDEX={group.group_index}",
            'TASK_INDEX="${SLURM_ARRAY_TASK_ID:-0}"',
            'printf "%s\\n" "[STAGE] batch_root=${BATCH_ROOT} group=${GROUP_INDEX} task=${TASK_INDEX}"',
            f"{_q(python_bin)} -m {_q(executor_module)} "
            '--batch-root "${BATCH_ROOT}" --group-index "${GROUP_INDEX}" --task-index "${TASK_INDEX}"',
            "",
        ]
    )
    return "\n".join(lines)
