from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import SchemaVersion, content_digest, read_json, stable_json, write_json
from ..plans import GroupPlan, StageBatchPlan
from .sbatch import _job_name, _q, _runtime_bootstrap_lines


def _submit_root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root) / "submit"


def _generation_id(batch: StageBatchPlan) -> str:
    payload = {
        "batch_id": batch.batch_id,
        "stage_name": batch.stage_name,
        "selected_runs": batch.selected_runs,
        "groups": [
            {
                "group_id": group.group_id,
                "stage_instance_ids": group.stage_instance_ids,
                "array_throttle": group.array_throttle,
                "resources": group.resources,
            }
            for group in batch.group_plans
        ],
        "dependencies": batch.budget_plan.get("dependencies", ()),
    }
    digest = content_digest(payload, prefix=12)
    return f"gen_{digest}"


def _generation_dir(batch: StageBatchPlan, generation_id: str) -> Path:
    return _submit_root(batch) / "generations" / generation_id


def _manifest_path(batch_root: Path) -> Path:
    return batch_root / "submit" / "submit_manifest.json"


def _instances_by_id(batch: StageBatchPlan) -> dict[str, Any]:
    return {instance.stage_instance_id: instance for instance in batch.stage_instances}


def _runtime_for_group(batch: StageBatchPlan, group: GroupPlan) -> dict[str, Any]:
    instances = _instances_by_id(batch)
    plans = {
        stable_json(instances[stage_instance_id].runtime_plan): instances[stage_instance_id].runtime_plan
        for stage_instance_id in group.stage_instance_ids
    }
    if len(plans) != 1:
        raise ValueError(f"group {group.group_id} mixes multiple runtime plans")
    return next(iter(plans.values()))


def _render_stage_group_sbatch(batch: StageBatchPlan, group: GroupPlan, *, generation_id: str | None = None) -> str:
    resources = group.resources
    runtime_plan = _runtime_for_group(batch, group)
    executor_plan = dict(runtime_plan.get("executor") or {})
    python_plan = dict(executor_plan.get("python") or {})
    python_bin = str(python_plan.get("bin") or "python3")
    executor_module = str(executor_plan.get("module") or "slurmforge.executor.stage")
    submit_dir = _submit_root(batch)
    log_dir = submit_dir / "logs" / (generation_id or "manual")
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={_job_name('sforge', batch.project, batch.stage_name, group.group_id)}",
        f"#SBATCH --output={_q(str(log_dir / (group.group_id + '-%A_%a.out')))}",
        f"#SBATCH --error={_q(str(log_dir / (group.group_id + '-%A_%a.err')))}",
    ]
    if resources.get("partition"):
        lines.append(f"#SBATCH --partition={resources['partition']}")
    if resources.get("account"):
        lines.append(f"#SBATCH --account={resources['account']}")
    if resources.get("qos"):
        lines.append(f"#SBATCH --qos={resources['qos']}")
    if resources.get("time_limit"):
        lines.append(f"#SBATCH --time={resources['time_limit']}")
    lines.extend(
        [
            f"#SBATCH --nodes={int(resources.get('nodes') or 1)}",
            "#SBATCH --ntasks-per-node=1",
            f"#SBATCH --cpus-per-task={int(resources.get('cpus_per_task') or 1)}",
        ]
    )
    if int(resources.get("gpus_per_node") or 0) > 0:
        lines.append(f"#SBATCH --gres=gpu:{int(resources.get('gpus_per_node') or 0)}")
    if resources.get("mem"):
        lines.append(f"#SBATCH --mem={resources['mem']}")
    if resources.get("constraint"):
        lines.append(f"#SBATCH --constraint={resources['constraint']}")
    for arg in resources.get("extra_sbatch_args") or ():
        lines.append(f"#SBATCH {arg}")
    throttle = ""
    if group.array_throttle is not None and group.array_throttle > 0 and group.array_throttle < group.array_size:
        throttle = f"%{group.array_throttle}"
    lines.extend(
        [
            f"#SBATCH --array=0-{group.array_size - 1}{throttle}",
            "",
            "set -euo pipefail",
            *_runtime_bootstrap_lines(runtime_plan),
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


def write_stage_submit_files(batch: StageBatchPlan) -> tuple[Path, ...]:
    root = Path(batch.submission_root)
    generation_id = _generation_id(batch)
    submit_dir = _generation_dir(batch, generation_id)
    (submit_dir / "logs").mkdir(parents=True, exist_ok=True)
    (_submit_root(batch) / "logs" / generation_id).mkdir(parents=True, exist_ok=True)
    sbatch_paths: list[Path] = []
    for group in batch.group_plans:
        path = submit_dir / f"{group.group_id}.sbatch"
        path.write_text(_render_stage_group_sbatch(batch, group, generation_id=generation_id), encoding="utf-8")
        path.chmod(0o755)
        sbatch_paths.append(path)
    submit_script = submit_dir / "submit.sh"
    submit_lines = ["#!/usr/bin/env bash", "set -euo pipefail"]
    job_vars: dict[str, str] = {}
    dependencies = {item["to_group"]: item for item in batch.budget_plan.get("dependencies", ())}
    for path in sbatch_paths:
        group_id = path.stem
        var_name = f"JOB_{group_id.upper()}"
        dep = dependencies.get(group_id)
        if dep is None:
            submit_lines.append(f'{var_name}="$(sbatch --parsable {_q(str(path))})"')
        else:
            from_groups = dep.get("from_groups") or [dep.get("from_group")]
            from_vars = [job_vars[str(item)] for item in from_groups if item]
            dependency_expr = ":".join(f"${{{item}}}" for item in from_vars)
            submit_lines.append(
                f'{var_name}="$(sbatch --parsable --dependency={dep["type"]}:{dependency_expr} {_q(str(path))})"'
            )
        submit_lines.append(f'printf "%s\\n" "{group_id}=${{{var_name}}}"')
        job_vars[group_id] = var_name
    submit_script.write_text("\n".join(submit_lines) + "\n", encoding="utf-8")
    submit_script.chmod(0o755)
    manifest = {
        "schema_version": SchemaVersion.SUBMIT_MANIFEST,
        "batch_id": batch.batch_id,
        "stage_name": batch.stage_name,
        "generation_id": generation_id,
        "generation_dir": str(submit_dir),
        "submit_script": str(submit_script),
        "groups": [
            {
                "group_id": group.group_id,
                "group_index": group.group_index,
                "sbatch_path": str(submit_dir / f"{group.group_id}.sbatch"),
                "array_size": group.array_size,
                "stage_instance_ids": group.stage_instance_ids,
            }
            for group in batch.group_plans
        ],
        "dependencies": batch.budget_plan.get("dependencies", ()),
    }
    write_json(_manifest_path(root), manifest)
    wrapper = _submit_root(batch) / "submit.sh"
    wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"exec {_q(str(submit_script))} \"$@\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return tuple(sbatch_paths)


def load_stage_submit_manifest(batch_root: Path) -> dict[str, Any]:
    return read_json(_manifest_path(batch_root))


def _stage_submit_paths_from_manifest(batch_root: Path) -> tuple[Path, ...]:
    manifest = load_stage_submit_manifest(batch_root)
    return tuple(Path(str(item["sbatch_path"])) for item in manifest.get("groups", ()))
