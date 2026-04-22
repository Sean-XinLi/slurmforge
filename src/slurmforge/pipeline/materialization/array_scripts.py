from __future__ import annotations

import os
import shlex
from typing import Any

from jinja2 import Environment

from ...text_safety import slurm_safe_job_name
from ..config.runtime import NotifyConfig, serialize_cluster_config, serialize_env_config
from ..planning import GpuBudgetPlan
from .slurm_deps import build_sbatch_dependency_clauses
from .context import ArrayGroupState, MaterializationLayout
from .grouping import resource_request_from_cluster
from .layout import map_to_staging


# Bash variable name holding the most recently submitted array group's JOB_ID.
# Serial policy chains subsequent groups with --dependency=afterany:<this>.
_SERIAL_PREV_JOB_VAR = "GROUP_PREV_JOB_ID"


def apply_gpu_budget_to_groups(
    groups: list[ArrayGroupState],
    plan: GpuBudgetPlan | None,
) -> None:
    """Copy throttle from a GpuBudgetPlan onto each ArrayGroupState.

    Group ordering is stable and matches the planner's group_id ordering, so
    we index by group_index.  A missing plan leaves ``array_throttle`` at 0
    (which the template reads as "no %K, use default --array=0-N").
    """
    if plan is None:
        return
    throttle_by_id = {g.group_id: g.throttle for g in plan.groups}
    for group in groups:
        group.array_throttle = int(throttle_by_id.get(group.group_index, 0))


def render_array_group_script(
    group: ArrayGroupState,
    *,
    env: Environment,
    layout: MaterializationLayout,
    project: str,
    experiment_name: str,
) -> None:
    array_template = env.get_template("sbatch_array_group.sh.j2")
    array_context = {
        "group_index": group.group_index,
        "array_size": group.count,
        "array_throttle": int(group.array_throttle or 0),
        "project": project,
        "experiment_name": experiment_name,
        "array_job_name": slurm_safe_job_name(f"{project}_{experiment_name}_arr{group.group_index:03d}"),
        "cluster": serialize_cluster_config(group.cluster),
        "array_log_dir": str(layout.array_log_dir),
        "batch_root": str(layout.final_batch_root),
        "run_plan_executor_bin": "sforge-run-plan-executor",
    }
    array_sbatch_staging = map_to_staging(
        group.array_sbatch,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    )
    from ..records.io_utils import atomic_write_text

    atomic_write_text(array_sbatch_staging, array_template.render(**array_context))
    os.chmod(array_sbatch_staging, 0o755)


def render_notify_script(
    *,
    env: Environment,
    layout: MaterializationLayout,
    project: str,
    experiment_name: str,
) -> None:
    notify_template = env.get_template("sbatch_notify.sh.j2")
    notify_context = {
        "project": project,
        "experiment_name": experiment_name,
    }
    notify_sbatch_staging = map_to_staging(
        layout.final_notify_sbatch,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    )
    from ..records.io_utils import atomic_write_text

    atomic_write_text(notify_sbatch_staging, notify_template.render(**notify_context))
    os.chmod(notify_sbatch_staging, 0o755)


def append_array_submit_lines(
    lines: list[str],
    group: ArrayGroupState,
    *,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]],
    serial_chain: bool,
    is_first_serial: bool,
) -> None:
    cluster_cfg = group.cluster
    lines.append(
        f'echo "[SUBMIT-ARRAY] group={group.group_index} tasks={group.count} '
        f'partition={cluster_cfg.partition} nodes={cluster_cfg.nodes} '
        f'gpus_per_node={cluster_cfg.gpus_per_node} '
        f'cpus_per_task={cluster_cfg.cpus_per_task} mem={cluster_cfg.mem} '
        f'file={group.array_sbatch}"'
    )
    lines.append(f'echo "[SUBMIT-ARRAY] reason={group.group_reason}"')

    sbatch_args: list[str] = ["--parsable"]

    # Merge external user-supplied dependencies with the serial-chain dependency
    # into a SINGLE --dependency flag.  Slurm treats repeated --dependency
    # arguments as replacements (later flag wins), so two separate flags would
    # silently drop the external dependencies — we build the merged clause list
    # first, then emit exactly one flag.
    dep_clauses: list[str] = build_sbatch_dependency_clauses(submit_dependencies)
    append_serial_clause = serial_chain and not is_first_serial
    if append_serial_clause:
        dep_clauses.append(f"afterany:${{{_SERIAL_PREV_JOB_VAR}}}")
    if dep_clauses:
        sbatch_args.append(f"--dependency={','.join(dep_clauses)}")
    # --kill-on-invalid-dep=no only matters for the serial clause: if an
    # earlier serial group is cancelled, we don't want that cancellation to
    # cascade and kill the rest of the independent experiments.  External
    # user-supplied dependencies do not get this behavior — they keep Slurm's
    # default cascade semantics, which matches what the user asked for.
    if append_serial_clause:
        sbatch_args.append("--kill-on-invalid-dep=no")

    sbatch_args.append(shlex.quote(str(group.array_sbatch)))

    lines.append(f"JOB_ID=$(sbatch {' '.join(sbatch_args)})")
    if notify_cfg is not None and notify_cfg.enabled:
        lines.append('JOB_IDS+=("${JOB_ID%%;*}")')
    if serial_chain:
        lines.append(f'{_SERIAL_PREV_JOB_VAR}="${{JOB_ID%%;*}}"')
    lines.append(f'echo "[SUBMITTED] group={group.group_index} job_id=${{JOB_ID}}"')


def build_array_group_meta(group: ArrayGroupState) -> dict[str, Any]:
    return {
        "group_index": group.group_index,
        "array_size": group.count,
        "array_throttle": int(group.array_throttle or 0),
        "sbatch_path": str(group.array_sbatch),
        "records_dir": str(group.records_dir),
        "cluster": serialize_cluster_config(group.cluster),
        "resource_request": resource_request_from_cluster(group.cluster),
        "group_signature": group.group_signature,
        "grouping_fields": group.grouping_fields,
        "group_reason": group.group_reason,
        "runtime_env": serialize_env_config(group.env),
        "run_indices": group.run_indices,
    }


def render_array_groups(
    groups_in_order: list[ArrayGroupState],
    *,
    env: Environment,
    layout: MaterializationLayout,
    project: str,
    experiment_name: str,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]],
    submit_lines: list[str],
    gpu_budget_plan: GpuBudgetPlan | None = None,
) -> list[dict[str, Any]]:
    apply_gpu_budget_to_groups(groups_in_order, gpu_budget_plan)
    serial_chain = (
        gpu_budget_plan is not None
        and gpu_budget_plan.policy_applied == "serialized_groups"
    )

    array_groups_meta: list[dict[str, Any]] = []
    is_first = True
    for group in groups_in_order:
        if group.count <= 0:
            continue
        render_array_group_script(
            group,
            env=env,
            layout=layout,
            project=project,
            experiment_name=experiment_name,
        )
        append_array_submit_lines(
            submit_lines,
            group,
            notify_cfg=notify_cfg,
            submit_dependencies=submit_dependencies,
            serial_chain=serial_chain,
            is_first_serial=is_first,
        )
        is_first = False
        array_groups_meta.append(build_array_group_meta(group))
    return array_groups_meta
