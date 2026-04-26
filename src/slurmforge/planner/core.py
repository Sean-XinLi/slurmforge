from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..resolver import default_stage_input_bindings
from ..schema import InputBinding, binding_is_ready_for_injection
from ..errors import ConfigContractError
from ..io import SchemaVersion, content_digest, to_jsonable
from ..plans import (
    ControllerPlan,
    GroupPlan,
    PipelinePlan,
    RunDefinition,
    StageBatchPlan,
    StageInstancePlan,
)
from ..spec import (
    ExperimentSpec,
    StageSpec,
    expand_run_definitions,
    normalize_matrix_path,
    parse_experiment_spec,
    stage_name_for_kind,
)
from ..overrides import deep_set


def materialize_run_spec(spec: ExperimentSpec, run: RunDefinition) -> ExperimentSpec:
    raw = copy.deepcopy(spec.raw)
    for key, value in run.matrix_assignments.items():
        deep_set(raw, normalize_matrix_path(raw, key), copy.deepcopy(value))
    return parse_experiment_spec(
        raw,
        config_path=spec.config_path,
        project_root=spec.project_root,
        forced_digest=spec.spec_snapshot_digest,
    )


def _entry_payload(spec: ExperimentSpec, stage: StageSpec) -> dict[str, Any]:
    workdir = Path(stage.entry.workdir)
    resolved_workdir = workdir if workdir.is_absolute() else spec.project_root / workdir
    return {
        "type": stage.entry.type,
        "script": stage.entry.script,
        "command": stage.entry.command,
        "workdir": str(resolved_workdir.resolve()),
        "args": copy.deepcopy(stage.entry.args),
    }


def _resource_payload(stage: StageSpec) -> dict[str, Any]:
    return {
        "partition": stage.resources.partition,
        "account": stage.resources.account,
        "qos": stage.resources.qos,
        "time_limit": stage.resources.time_limit,
        "nodes": stage.resources.nodes,
        "gpus_per_node": stage.resources.gpus_per_node,
        "cpus_per_task": stage.resources.cpus_per_task,
        "mem": stage.resources.mem,
        "constraint": stage.resources.constraint,
        "extra_sbatch_args": list(stage.resources.extra_sbatch_args),
    }


def _executor_runtime_payload(spec: ExperimentSpec) -> dict[str, Any]:
    return {
        "python": {
            "bin": spec.runtime.executor.python.bin,
            "min_version": spec.runtime.executor.python.min_version,
        },
        "module": spec.runtime.executor.executor_module,
        "bootstrap_scope": spec.runtime.executor.bootstrap_scope,
        "bootstrap_steps": copy.deepcopy(spec.runtime.executor.bootstrap_steps),
        "env": copy.deepcopy(spec.runtime.executor.env),
    }


def _runtime_payload(spec: ExperimentSpec, stage: StageSpec) -> dict[str, Any]:
    user_runtime = spec.runtime.user[stage.runtime]
    return {
        "executor": _executor_runtime_payload(spec),
        "user": {
            "name": stage.runtime,
            "python": {
                "bin": user_runtime.python.bin,
                "min_version": user_runtime.python.min_version,
            },
            "env": copy.deepcopy(user_runtime.env),
        },
    }


def _auto_int(value: Any, default: int) -> int:
    if value in (None, "", "auto"):
        return int(default)
    return int(value)


def _launcher_payload(stage: StageSpec) -> dict[str, Any]:
    resources = _resource_payload(stage)
    launcher_type = stage.launcher.type
    options = copy.deepcopy(stage.launcher.options)
    if launcher_type == "torchrun":
        nodes = int(resources.get("nodes") or 1)
        gpus = int(resources.get("gpus_per_node") or 0)
        nproc_default = gpus if gpus > 0 else 1
        mode = str(options.get("mode") or ("multi_node" if nodes > 1 else "single_node"))
        rendezvous = dict(options.get("rendezvous") or {})
        port = rendezvous.get("port", options.get("master_port", 29500))
        payload = {
            "type": "torchrun",
            "mode": mode,
            "nnodes": _auto_int(options.get("nnodes") or options.get("nodes") or "auto", nodes),
            "nproc_per_node": _auto_int(
                options.get("nproc_per_node") or options.get("processes_per_node") or "auto",
                nproc_default,
            ),
            "rendezvous": {
                "backend": str(rendezvous.get("backend") or "c10d"),
                "endpoint": str(rendezvous.get("endpoint") or "auto"),
                "port": int(port),
            },
            "srun_args": [str(item) for item in options.get("srun_args") or ()],
        }
        if options.get("master_port") is not None:
            payload["master_port"] = int(options["master_port"])
        return payload
    if launcher_type in {"srun", "mpirun"}:
        args = options.get("args") or ()
        return {"type": launcher_type, "args": [str(item) for item in args]}
    return {"type": launcher_type}


def _artifact_store_payload(spec: ExperimentSpec) -> dict[str, Any]:
    return {
        "strategy": spec.artifact_store.strategy,
        "fallback_strategy": spec.artifact_store.fallback_strategy,
        "verify_digest": spec.artifact_store.verify_digest,
        "fail_on_verify_error": spec.artifact_store.fail_on_verify_error,
    }


def _default_bindings(spec: ExperimentSpec, run: RunDefinition, stage: StageSpec) -> tuple[InputBinding, ...]:
    return default_stage_input_bindings(spec, run, stage)


def _group_key(instance: StageInstancePlan) -> str:
    payload = {"resources": instance.resources, "runtime_plan": instance.runtime_plan}
    return content_digest(payload, prefix=16)


def _batch_id(stage_name: str, runs: tuple[RunDefinition, ...], source_ref: str, spec_digest: str) -> str:
    payload = {
        "stage_name": stage_name,
        "run_ids": [run.run_id for run in runs],
        "source_ref": source_ref,
        "spec_snapshot_digest": spec_digest,
    }
    digest = content_digest(payload, prefix=12)
    return f"{stage_name}_batch_{digest}"


def _pipeline_id(spec: ExperimentSpec, runs: tuple[RunDefinition, ...], stage_order: tuple[str, ...]) -> str:
    payload = {
        "stage_order": stage_order,
        "run_ids": [run.run_id for run in runs],
        "spec_snapshot_digest": spec.spec_snapshot_digest,
    }
    digest = content_digest(payload, prefix=12)
    return f"pipeline_{digest}"


def _group_instances(
    instances: tuple[StageInstancePlan, ...],
) -> tuple[GroupPlan, ...]:
    by_key: dict[str, list[StageInstancePlan]] = {}
    resources_by_key: dict[str, dict[str, Any]] = {}
    for instance in instances:
        key = _group_key(instance)
        by_key.setdefault(key, []).append(instance)
        resources_by_key[key] = instance.resources
    groups: list[GroupPlan] = []
    for group_index, key in enumerate(sorted(by_key), start=1):
        items = tuple(sorted(by_key[key], key=lambda item: item.run_index))
        resources = resources_by_key[key]
        gpus_per_task = int(resources.get("nodes") or 1) * int(resources.get("gpus_per_node") or 0)
        groups.append(
            GroupPlan(
                group_id=f"group_{group_index:03d}",
                group_index=group_index,
                resource_key=key,
                resources=copy.deepcopy(resources),
                stage_instance_ids=tuple(item.stage_instance_id for item in items),
                run_ids=tuple(item.run_id for item in items),
                array_size=len(items),
                array_throttle=None,
                gpus_per_task=gpus_per_task,
            )
        )
    return tuple(groups)


def _allocate_wave_throttles(
    wave_groups: list[GroupPlan],
    *,
    max_available_gpus: int,
) -> dict[str, int]:
    throttles = {group.group_id: 1 for group in wave_groups}
    used = sum(group.gpus_per_task for group in wave_groups)
    remaining = max_available_gpus - used
    for group in wave_groups:
        if remaining <= 0:
            break
        extra = min(group.array_size - 1, remaining // group.gpus_per_task)
        if extra > 0:
            throttles[group.group_id] += extra
            remaining -= extra * group.gpus_per_task
    return throttles


def _apply_budget_plan(
    groups: tuple[GroupPlan, ...],
    *,
    max_available_gpus: int,
    overflow_policy: str,
) -> tuple[tuple[GroupPlan, ...], dict[str, Any]]:
    gpu_groups = [group for group in groups if group.gpus_per_task > 0]
    cpu_groups = [group for group in groups if group.gpus_per_task <= 0]
    warnings: list[str] = []
    dependencies: list[dict[str, str]] = []
    policy_applied = "none"
    waves: list[dict[str, Any]] = []
    throttles: dict[str, int | None] = {group.group_id: None for group in groups}

    if max_available_gpus <= 0 or not gpu_groups:
        policy_applied = "unlimited" if gpu_groups else "none"
        for group in groups:
            throttles[group.group_id] = None
    else:
        for group in gpu_groups:
            if group.gpus_per_task > max_available_gpus:
                raise ConfigContractError(
                    f"{group.stage_instance_ids[0]} needs {group.gpus_per_task} GPUs per task, "
                    f"above dispatch.max_available_gpus={max_available_gpus}"
                )
        current: list[GroupPlan] = []
        current_used = 0
        for group in gpu_groups:
            if current and current_used + group.gpus_per_task > max_available_gpus:
                wave_throttles = _allocate_wave_throttles(current, max_available_gpus=max_available_gpus)
                waves.append(
                    {
                        "wave_id": f"wave_{len(waves) + 1:03d}",
                        "groups": [
                            {
                                "group_id": item.group_id,
                                "gpus_per_task": item.gpus_per_task,
                                "array_size": item.array_size,
                                "array_throttle": wave_throttles[item.group_id],
                            }
                            for item in current
                        ],
                        "total_wave_gpus": sum(
                            item.gpus_per_task * wave_throttles[item.group_id] for item in current
                        ),
                    }
                )
                throttles.update(wave_throttles)
                current = []
                current_used = 0
            current.append(group)
            current_used += group.gpus_per_task
        if current:
            wave_throttles = _allocate_wave_throttles(current, max_available_gpus=max_available_gpus)
            waves.append(
                {
                    "wave_id": f"wave_{len(waves) + 1:03d}",
                    "groups": [
                        {
                            "group_id": item.group_id,
                            "gpus_per_task": item.gpus_per_task,
                            "array_size": item.array_size,
                            "array_throttle": wave_throttles[item.group_id],
                        }
                        for item in current
                    ],
                    "total_wave_gpus": sum(item.gpus_per_task * wave_throttles[item.group_id] for item in current),
                }
            )
            throttles.update(wave_throttles)
        policy_applied = "global_waves" if len(waves) > 1 else "global_shared_budget"
        for previous, current_wave in zip(waves, waves[1:]):
            from_groups = [item["group_id"] for item in previous["groups"]]
            for item in current_wave["groups"]:
                dependencies.append(
                    {
                        "from_groups": from_groups,
                        "to_group": item["group_id"],
                        "type": "afterany",
                        "from_wave": previous["wave_id"],
                        "to_wave": current_wave["wave_id"],
                    }
                )
        if overflow_policy == "best_effort":
            warnings.append("best_effort accepted; global wave planning still enforces the GPU ceiling")
        if overflow_policy == "error" and len(waves) > 1:
            raise ConfigContractError(
                "stage batch requires multiple GPU waves to satisfy dispatch.max_available_gpus; "
                "use overflow_policy=serialize_groups"
            )

    updated_groups = tuple(
        replace(group, array_throttle=throttles[group.group_id])
        for group in groups
    )
    budget_plan = {
        "schema_version": SchemaVersion.BUDGET_PLAN,
        "max_available_gpus": max_available_gpus,
        "overflow_policy": overflow_policy,
        "policy_applied": policy_applied,
        "waves": waves,
        "groups": [
            {
                "group_id": group.group_id,
                "gpus_per_task": group.gpus_per_task,
                "array_size": group.array_size,
                "array_throttle": throttles[group.group_id],
                "budgeted_gpus": None
                if throttles[group.group_id] is None
                else group.gpus_per_task * int(throttles[group.group_id] or 0),
            }
            for group in groups
        ],
        "cpu_groups": [group.group_id for group in cpu_groups],
        "dependencies": dependencies,
        "warnings": warnings,
    }
    return updated_groups, budget_plan


def compile_stage_batch(
    spec: ExperimentSpec,
    *,
    stage_name: str,
    runs: tuple[RunDefinition, ...] | None = None,
    submission_root: Path | None = None,
    source_ref: str = "config",
    input_bindings_by_run: dict[str, tuple[InputBinding, ...]] | None = None,
    batch_id: str | None = None,
) -> StageBatchPlan:
    selected_runs = expand_run_definitions(spec) if runs is None else runs
    if not selected_runs:
        raise ConfigContractError("Stage batch requires at least one run")
    if stage_name not in spec.enabled_stages:
        raise ConfigContractError(f"Stage `{stage_name}` is not enabled")
    actual_batch_id = batch_id or _batch_id(stage_name, selected_runs, source_ref, spec.spec_snapshot_digest)
    root = submission_root or spec.storage_root / spec.project / spec.experiment / actual_batch_id
    instances: list[StageInstancePlan] = []
    for run in selected_runs:
        run_spec = materialize_run_spec(spec, run)
        stage = run_spec.enabled_stages[stage_name]
        if input_bindings_by_run is None:
            bindings = None
        else:
            if run.run_id not in input_bindings_by_run:
                raise ConfigContractError(
                    f"Input bindings were provided for stage `{stage_name}`, but run `{run.run_id}` is missing"
                )
            bindings = input_bindings_by_run[run.run_id]
            for binding in bindings:
                if binding.inject.get("required") and not binding_is_ready_for_injection(binding):
                    raise ConfigContractError(
                        f"Required input `{binding.input_name}` for run `{run.run_id}` is not ready for injection"
                    )
        stage_instance_id = f"{run.run_id}.{stage.name}"
        instances.append(
            StageInstancePlan(
                stage_instance_id=stage_instance_id,
                run_id=run.run_id,
                run_index=run.run_index,
                stage_name=stage.name,
                stage_kind=stage.kind,
                entry=_entry_payload(run_spec, stage),
                resources=_resource_payload(stage),
                runtime_plan=_runtime_payload(run_spec, stage),
                launcher_plan=_launcher_payload(stage),
                artifact_store_plan=_artifact_store_payload(run_spec),
                input_bindings=bindings if bindings is not None else _default_bindings(run_spec, run, stage),
                output_contract=copy.deepcopy(stage.outputs),
                lineage={
                    "project": spec.project,
                    "experiment": spec.experiment,
                    "config_path": str(spec.config_path),
                    "project_root": str(spec.project_root),
                    "source_ref": source_ref,
                },
                matrix_assignments=copy.deepcopy(run.matrix_assignments),
                spec_snapshot_digest=spec.spec_snapshot_digest,
                run_dir_rel=f"runs/{run.run_id}",
            )
        )
    stage_instances = tuple(sorted(instances, key=lambda item: item.run_index))
    groups = _group_instances(stage_instances)
    groups, budget_plan = _apply_budget_plan(
        groups,
        max_available_gpus=spec.dispatch.max_available_gpus,
        overflow_policy=spec.dispatch.overflow_policy,
    )
    return StageBatchPlan(
        batch_id=actual_batch_id,
        stage_name=stage_name,
        project=spec.project,
        experiment=spec.experiment,
        selected_runs=tuple(run.run_id for run in selected_runs),
        stage_instances=stage_instances,
        group_plans=groups,
        submission_root=str(root.resolve()),
        source_ref=source_ref,
        spec_snapshot_digest=spec.spec_snapshot_digest,
        budget_plan=budget_plan,
    )


def compile_stage_batch_for_kind(
    spec: ExperimentSpec,
    *,
    kind: str,
    runs: tuple[RunDefinition, ...] | None = None,
    submission_root: Path | None = None,
    source_ref: str = "config",
    input_bindings_by_run: dict[str, tuple[InputBinding, ...]] | None = None,
) -> StageBatchPlan:
    return compile_stage_batch(
        spec,
        stage_name=stage_name_for_kind(spec, kind),
        runs=runs,
        submission_root=submission_root,
        source_ref=source_ref,
        input_bindings_by_run=input_bindings_by_run,
    )


def compile_pipeline_plan(spec: ExperimentSpec, *, submission_root: Path | None = None) -> PipelinePlan:
    runs = expand_run_definitions(spec)
    stage_order = spec.stage_order()
    if stage_order != ("train", "eval"):
        raise ConfigContractError("Pipeline runs require enabled `stages.train` and `stages.eval` in train -> eval order")
    pipeline_id = _pipeline_id(spec, runs, stage_order)
    root = (submission_root or spec.storage_root / spec.project / spec.experiment / pipeline_id).resolve()
    stage_batches: dict[str, StageBatchPlan] = {}
    for stage_name in stage_order:
        stage_root = root / "stage_batches" / stage_name
        stage_batches[stage_name] = compile_stage_batch(
            spec,
            stage_name=stage_name,
            runs=runs,
            submission_root=stage_root,
            source_ref=f"pipeline:{pipeline_id}",
            batch_id=f"{pipeline_id}_{stage_name}",
        )
    controller_plan = ControllerPlan(
        pipeline_id=pipeline_id,
        stage_order=stage_order,
        config_path=str(spec.config_path),
        root_dir=str(root),
        resources={
            "partition": spec.orchestration.controller_partition,
            "cpus": spec.orchestration.controller_cpus,
            "mem": spec.orchestration.controller_mem,
            "time_limit": spec.orchestration.controller_time_limit,
        },
        runtime_plan={
            "executor": _executor_runtime_payload(spec),
        },
    )
    return PipelinePlan(
        pipeline_id=pipeline_id,
        stage_order=stage_order,
        run_set=tuple(run.run_id for run in runs),
        root_dir=str(root),
        controller_plan=controller_plan,
        stage_batches=stage_batches,
        spec_snapshot_digest=spec.spec_snapshot_digest,
    )


def summarize_stage_batch(batch: StageBatchPlan) -> list[str]:
    lines = [
        f"[PLAN] stage_batch={batch.batch_id} stage={batch.stage_name} runs={len(batch.selected_runs)} root={batch.submission_root}",
    ]
    for group in batch.group_plans:
        throttle = "-" if group.array_throttle is None else str(group.array_throttle)
        lines.append(
            f"[PLAN] {group.group_id} runs={group.array_size} gpus_per_task={group.gpus_per_task} throttle={throttle}"
        )
    for dep in batch.budget_plan.get("dependencies", ()):
        from_groups = dep.get("from_groups") or [dep.get("from_group")]
        from_text = ",".join(str(item) for item in from_groups if item)
        lines.append(
            f"[PLAN] dependency {dep['to_group']} after {from_text} type={dep['type']}"
        )
    for warning in batch.budget_plan.get("warnings", ()):
        lines.append(f"[WARN] {warning}")
    return lines


def summarize_pipeline_plan(plan: PipelinePlan) -> list[str]:
    lines = [
        f"[PLAN] pipeline={plan.pipeline_id} stages={' -> '.join(plan.stage_order)} runs={len(plan.run_set)} root={plan.root_dir}",
    ]
    for stage_name in plan.stage_order:
        lines.extend(summarize_stage_batch(plan.stage_batches[stage_name]))
    return lines


def serialize_plan(plan: StageBatchPlan | PipelinePlan) -> dict[str, Any]:
    return to_jsonable(plan)
