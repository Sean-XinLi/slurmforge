from __future__ import annotations

from pathlib import Path

from ..config_contract.option_sets import INPUT_EXPECTS_VALUE
from ..contracts import InputResolution, ResolvedInput, resolved_kind_for_output_kind
from ..errors import ConfigContractError
from ..plans.outputs import OutputRef, StageOutputsRecord
from ..spec import StageInputSpec


def output_ref(record: StageOutputsRecord, output_name: str) -> OutputRef | None:
    output = record.outputs.get(output_name)
    if output is None or not output.path:
        return None
    return output


def producer_root_from_run_dir(run_dir: Path) -> Path:
    if run_dir.parent.name == "runs":
        return run_dir.parent.parent.resolve()
    return run_dir.parent.resolve()


def resolved_output(output: OutputRef) -> ResolvedInput:
    resolved_kind = resolved_kind_for_output_kind(output.kind, output.cardinality)
    if resolved_kind == INPUT_EXPECTS_VALUE:
        return ResolvedInput(
            kind=INPUT_EXPECTS_VALUE,
            path=output.path,
            value=output.value,
            digest=output.digest,
            source_output_kind=output.kind,
            producer_stage_instance_id=output.producer_stage_instance_id,
        )
    return ResolvedInput(
        kind=resolved_kind,
        path=output.path,
        digest=output.digest,
        source_output_kind=output.kind or output.output_name,
        producer_stage_instance_id=output.producer_stage_instance_id,
    )


def upstream_resolution(
    *,
    producer_root: Path,
    run_dir: Path,
    stage_instance_id: str,
    run_id: str,
    stage_name: str,
    output_name: str,
    output: OutputRef,
) -> InputResolution:
    return InputResolution(
        kind="upstream_output",
        state="resolved",
        producer_root=str(producer_root.resolve()),
        producer_run_dir=str(run_dir.resolve()),
        producer_stage_instance_id=stage_instance_id,
        producer_run_id=run_id,
        producer_stage_name=stage_name,
        output_name=output_name,
        output_path=output.path,
        output_digest=output.digest or output.managed_digest,
        selection_reason=output.selection_reason,
    )


def producer_output_for_input(
    input_spec: StageInputSpec, *, producer_stage_name: str
) -> tuple[str, str]:
    if input_spec.source.kind == "upstream_output":
        upstream_stage = input_spec.source.stage
        output_name = input_spec.source.output
        if upstream_stage != producer_stage_name:
            raise ConfigContractError(
                f"`{input_spec.name}` expects upstream stage `{upstream_stage}`, "
                f"but source batch is `{producer_stage_name}`"
            )
        return upstream_stage, output_name
    raise ConfigContractError(
        f"`{input_spec.name}` source kind `{input_spec.source.kind}` cannot be bound from an upstream batch"
    )
