from __future__ import annotations

from pathlib import Path

from ...contracts import InputBinding, InputSource
from ...errors import ConfigContractError
from ...contracts import RunDefinition
from ...spec import ExperimentSpec
from ...spec.queries import stage_name_for_kind, stage_source_input_name
from ...outputs.records import load_stage_outputs
from ...storage.plan_reader import plan_for_run_dir
from ..binding_builders import input_inject, path_binding_for_input
from ..output_refs import (
    output_ref,
    producer_output_for_input,
    producer_root_from_run_dir,
    resolved_output,
    upstream_resolution,
)


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
    selected_input = input_name or stage_source_input_name(
        spec, stage_name=consumer_stage
    )
    input_spec = spec.enabled_stages[consumer_stage].inputs.get(selected_input)
    if input_spec is None:
        raise ConfigContractError(
            f"`stages.{consumer_stage}.inputs.{selected_input}` is required"
        )
    _producer_stage, output_name = producer_output_for_input(
        input_spec, producer_stage_name=instance.stage_name
    )
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
                    source=InputSource(
                        kind="upstream_output",
                        stage=instance.stage_name,
                        output=output_name,
                    ),
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
