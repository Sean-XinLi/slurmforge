from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..plans import RunDefinition
from ..contracts import InputBinding, InputSource, ResolvedInput
from ..spec import ExperimentSpec, expand_run_definitions, stage_name_for_kind, stage_source_input_name
from ..status import read_stage_status
from ..storage.loader import (
    load_stage_batch_plan,
    load_stage_outputs,
    plan_for_run_dir,
    run_definitions_from_stage_batch,
)
from .binding_builders import (
    input_inject,
    path_binding_for_input,
)
from .output_refs import (
    output_ref,
    producer_output_for_input,
    producer_root_from_run_dir,
    resolved_output,
    upstream_resolution,
)


def explicit_input_bindings(
    spec: ExperimentSpec,
    input_name: str,
    path: Path,
    *,
    stage_name: str | None = None,
    runs: tuple[RunDefinition, ...] | None = None,
    source_role: str = "",
) -> tuple[tuple[RunDefinition, ...], dict[str, tuple[InputBinding, ...]]]:
    consumer_stage = stage_name or stage_name_for_kind(spec, "eval")
    input_spec = spec.enabled_stages[consumer_stage].inputs.get(input_name)
    if input_spec is None:
        raise ConfigContractError(f"`stages.{consumer_stage}.inputs.{input_name}` is required")
    if input_spec.expects not in {"path", "manifest"}:
        raise ConfigContractError(f"`{input_name}` expects {input_spec.expects}; explicit path inputs require path or manifest")
    source_path = path.expanduser()
    resolved = source_path.resolve() if source_path.is_absolute() else (spec.project_root / source_path).resolve()
    if not resolved.exists() or not resolved.is_file():
        raise ConfigContractError(f"Input path does not exist: {resolved}")
    selected_runs = runs or expand_run_definitions(spec)
    inject = input_inject(spec, stage_name=consumer_stage, input_name=input_name)
    resolution = {
        "kind": "external_path",
        "resolved": {"kind": input_spec.expects, "path": str(resolved)},
        "source_exists": True,
    }
    if source_role:
        resolution["source_role"] = source_role
    bindings = {
        run.run_id: (
            InputBinding(
                input_name=input_name,
                source=InputSource(kind="external_path", path=str(resolved)),
                expects=input_spec.expects,
                resolved=ResolvedInput(kind=input_spec.expects, path=str(resolved)),
                inject=inject,
                resolution=resolution,
            ),
        )
        for run in selected_runs
    }
    return selected_runs, bindings


def upstream_bindings_from_stage_batch(
    spec: ExperimentSpec,
    producer_batch_root: Path,
    *,
    consumer_stage_name: str | None = None,
    input_name: str | None = None,
) -> tuple[tuple[RunDefinition, ...], dict[str, tuple[InputBinding, ...]]]:
    batch = load_stage_batch_plan(producer_batch_root)
    consumer_stage = consumer_stage_name or stage_name_for_kind(spec, "eval")
    selected_input = input_name or stage_source_input_name(spec, stage_name=consumer_stage)
    input_spec = spec.enabled_stages[consumer_stage].inputs.get(selected_input)
    if input_spec is None:
        raise ConfigContractError(f"`stages.{consumer_stage}.inputs.{selected_input}` is required")
    _producer_stage, output_name = producer_output_for_input(input_spec, producer_stage_name=batch.stage_name)
    run_defs_by_id = {run.run_id: run for run in run_definitions_from_stage_batch(batch)}
    inject = input_inject(spec, stage_name=consumer_stage, input_name=selected_input)
    selected_runs: list[RunDefinition] = []
    bindings: dict[str, tuple[InputBinding, ...]] = {}
    for instance in sorted(batch.stage_instances, key=lambda item: item.run_index):
        run_dir = producer_batch_root / instance.run_dir_rel
        status = read_stage_status(run_dir)
        if status is None or status.state != "success":
            continue
        outputs = load_stage_outputs(run_dir)
        if outputs is None:
            continue
        output = output_ref(outputs, output_name)
        if output is None:
            continue
        run_def = run_defs_by_id[instance.run_id]
        selected_runs.append(run_def)
        bindings[run_def.run_id] = (
            path_binding_for_input(
                input_name=selected_input,
                inject=inject,
                source=InputSource(kind="upstream_output", stage=instance.stage_name, output=output_name),
                expects=input_spec.expects,
                resolved=resolved_output(output),
                resolution=upstream_resolution(
                    producer_root=producer_batch_root,
                    run_dir=run_dir,
                    stage_instance_id=instance.stage_instance_id,
                    run_id=instance.run_id,
                    stage_name=instance.stage_name,
                    output_name=output_name,
                    output=output,
                ),
            ),
        )
    if not selected_runs:
        raise ConfigContractError(f"No `{output_name}` outputs found under {producer_batch_root}")
    return tuple(selected_runs), bindings


def upstream_bindings_from_train_batch(
    spec: ExperimentSpec,
    train_batch_root: Path,
    *,
    input_name: str | None = None,
) -> tuple[tuple[RunDefinition, ...], dict[str, tuple[InputBinding, ...]]]:
    return upstream_bindings_from_stage_batch(spec, train_batch_root, input_name=input_name)


def upstream_bindings_from_run(
    spec: ExperimentSpec,
    run_dir: Path,
    *,
    consumer_stage_name: str | None = None,
    input_name: str | None = None,
) -> tuple[tuple[RunDefinition, ...], dict[str, tuple[InputBinding, ...]]]:
    instance = plan_for_run_dir(run_dir)
    outputs = load_stage_outputs(run_dir)
    if instance is None or outputs is None:
        raise ConfigContractError(f"No stage plan and outputs found under {run_dir}")
    consumer_stage = consumer_stage_name or stage_name_for_kind(spec, "eval")
    selected_input = input_name or stage_source_input_name(spec, stage_name=consumer_stage)
    input_spec = spec.enabled_stages[consumer_stage].inputs.get(selected_input)
    if input_spec is None:
        raise ConfigContractError(f"`stages.{consumer_stage}.inputs.{selected_input}` is required")
    _producer_stage, output_name = producer_output_for_input(input_spec, producer_stage_name=instance.stage_name)
    output = output_ref(outputs, output_name)
    if output is None:
        raise ConfigContractError(f"No `{output_name}` output found under {run_dir}")
    run = RunDefinition(
        run_id=instance.run_id,
        run_index=instance.run_index,
        run_overrides=dict(instance.run_overrides),
        spec_snapshot_digest=instance.spec_snapshot_digest,
    )
    inject = input_inject(spec, stage_name=consumer_stage, input_name=selected_input)
    return (
        (run,),
        {
            run.run_id: (
                path_binding_for_input(
                    input_name=selected_input,
                    inject=inject,
                    source=InputSource(kind="upstream_output", stage=instance.stage_name, output=output_name),
                    expects=input_spec.expects,
                    resolved=resolved_output(output),
                    resolution=upstream_resolution(
                        producer_root=producer_root_from_run_dir(run_dir),
                        run_dir=run_dir,
                        stage_instance_id=instance.stage_instance_id,
                        run_id=instance.run_id,
                        stage_name=instance.stage_name,
                        output_name=output_name,
                        output=output,
                    ),
                ),
            )
        },
    )
