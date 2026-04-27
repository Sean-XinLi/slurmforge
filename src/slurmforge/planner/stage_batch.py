from __future__ import annotations

import copy
from pathlib import Path

from ..errors import ConfigContractError
from ..overrides import deep_set
from ..plans import RunDefinition, StageBatchPlan, StageInstancePlan
from ..schema import InputBinding, binding_is_ready_for_injection
from ..spec import (
    ExperimentSpec,
    expand_run_definitions,
    parse_experiment_spec,
    stage_name_for_kind,
)
from ..spec.queries import normalize_run_path
from .budget import apply_budget_plan, group_stage_instances
from .identifiers import batch_id as make_batch_id
from .payloads import (
    artifact_store_payload,
    before_payload,
    default_bindings,
    entry_payload,
    environment_payload,
    launcher_payload,
    notification_payload,
    resource_payload,
    resource_sizing_payload,
    runtime_payload,
)


def materialize_run_spec(spec: ExperimentSpec, run: RunDefinition) -> ExperimentSpec:
    raw = copy.deepcopy(spec.raw)
    for key, value in run.run_overrides.items():
        deep_set(raw, normalize_run_path(raw, key), copy.deepcopy(value))
    return parse_experiment_spec(
        raw,
        config_path=spec.config_path,
        project_root=spec.project_root,
        forced_digest=spec.spec_snapshot_digest,
    )


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
    actual_batch_id = batch_id or make_batch_id(stage_name, selected_runs, source_ref, spec.spec_snapshot_digest)
    root = submission_root or spec.storage_root / spec.project / spec.experiment / actual_batch_id
    stage_instances = _compile_stage_instances(
        spec,
        stage_name=stage_name,
        selected_runs=selected_runs,
        source_ref=source_ref,
        input_bindings_by_run=input_bindings_by_run,
    )
    groups = group_stage_instances(stage_instances)
    groups, budget_plan = apply_budget_plan(
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
        notification_plan=notification_payload(spec),
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


def _compile_stage_instances(
    spec: ExperimentSpec,
    *,
    stage_name: str,
    selected_runs: tuple[RunDefinition, ...],
    source_ref: str,
    input_bindings_by_run: dict[str, tuple[InputBinding, ...]] | None,
) -> tuple[StageInstancePlan, ...]:
    instances: list[StageInstancePlan] = []
    for run in selected_runs:
        run_spec = materialize_run_spec(spec, run)
        stage = run_spec.enabled_stages[stage_name]
        resource_sizing = resource_sizing_payload(run_spec, stage)
        resources = resource_payload(run_spec, stage, resource_sizing)
        bindings = _input_bindings_for_run(
            run=run,
            spec=run_spec,
            stage=stage,
            input_bindings_by_run=input_bindings_by_run,
        )
        stage_instance_id = f"{run.run_id}.{stage.name}"
        instances.append(
            StageInstancePlan(
                stage_instance_id=stage_instance_id,
                run_id=run.run_id,
                run_index=run.run_index,
                stage_name=stage.name,
                stage_kind=stage.kind,
                entry=entry_payload(run_spec, stage),
                resources=resources,
                runtime_plan=runtime_payload(run_spec, stage),
                environment_name=stage.environment,
                environment_plan=environment_payload(run_spec, stage.environment),
                before_steps=before_payload(stage),
                launcher_plan=launcher_payload(stage, resources),
                artifact_store_plan=artifact_store_payload(run_spec),
                input_bindings=bindings,
                output_contract=copy.deepcopy(stage.outputs),
                lineage={
                    "project": spec.project,
                    "experiment": spec.experiment,
                    "config_path": str(spec.config_path),
                    "project_root": str(spec.project_root),
                    "source_ref": source_ref,
                },
                run_overrides=copy.deepcopy(run.run_overrides),
                resource_sizing=copy.deepcopy(resource_sizing),
                spec_snapshot_digest=spec.spec_snapshot_digest,
                run_dir_rel=f"runs/{run.run_id}",
            )
        )
    return tuple(sorted(instances, key=lambda item: item.run_index))


def _input_bindings_for_run(
    *,
    run: RunDefinition,
    spec: ExperimentSpec,
    stage,
    input_bindings_by_run: dict[str, tuple[InputBinding, ...]] | None,
) -> tuple[InputBinding, ...]:
    if input_bindings_by_run is None:
        return default_bindings(spec, run, stage)
    if run.run_id not in input_bindings_by_run:
        raise ConfigContractError(
            f"Input bindings were provided for stage `{stage.name}`, but run `{run.run_id}` is missing"
        )
    bindings = input_bindings_by_run[run.run_id]
    for binding in bindings:
        if binding.inject.get("required") and not binding_is_ready_for_injection(binding):
            raise ConfigContractError(
                f"Required input `{binding.input_name}` for run `{run.run_id}` is not ready for injection"
            )
    return bindings
