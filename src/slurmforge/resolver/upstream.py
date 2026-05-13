from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from ..contracts import (
    InputBinding,
    InputResolution,
    InputSource,
    ResolvedInput,
    resolved_payload_present,
)
from ..errors import ConfigContractError
from ..lineage.query import find_bound_input, iter_lineage_source_roots
from ..lineage.records import LineageInputSourceRecord
from ..root_model.runs import iter_runtime_stage_run_dirs
from ..outputs.records import load_stage_outputs
from ..plans.outputs import OutputRef
from ..storage.plan_reader import plan_for_run_dir
from .output_refs import (
    output_ref,
    producer_root_from_run_dir,
    resolved_output,
    upstream_resolution,
)


@dataclass(frozen=True)
class FoundInputBinding:
    source: InputSource
    resolved: ResolvedInput
    resolution: InputResolution


def _output_resolution(
    *,
    root: Path,
    run_dir: Path,
    lineage_ref: str,
    output_name: str,
    output: OutputRef,
    producer_stage_instance_id: str,
    producer_run_id: str,
    producer_stage_name: str,
) -> FoundInputBinding:
    resolution = upstream_resolution(
        producer_root=producer_root_from_run_dir(run_dir),
        run_dir=run_dir,
        stage_instance_id=producer_stage_instance_id,
        run_id=producer_run_id,
        stage_name=producer_stage_name,
        output_name=output_name,
        output=output,
    )
    return FoundInputBinding(
        source=InputSource(
            kind="upstream_output", stage=producer_stage_name, output=output_name
        ),
        resolved=resolved_output(output),
        resolution=replace(
            resolution,
            searched_root=str(root.resolve()),
            lineage_ref=lineage_ref,
        ),
    )


def _find_upstream_output_direct(
    root: Path, lineage_ref: str
) -> FoundInputBinding | None:
    if ":" not in lineage_ref:
        return None
    producer, output_name = lineage_ref.split(":", 1)
    try:
        run_dirs = tuple(iter_runtime_stage_run_dirs(root))
    except ConfigContractError:
        return None
    for run_dir in run_dirs:
        plan = plan_for_run_dir(run_dir)
        if plan is None or plan.stage_instance_id != producer:
            continue
        outputs = load_stage_outputs(run_dir)
        if outputs is None:
            return None
        output = output_ref(outputs, output_name)
        if output is not None:
            return _output_resolution(
                root=root,
                run_dir=run_dir,
                lineage_ref=lineage_ref,
                output_name=output_name,
                output=output,
                producer_stage_instance_id=plan.stage_instance_id,
                producer_run_id=plan.run_id,
                producer_stage_name=plan.stage_name,
            )
    return None


def _record_resolution(
    root: Path, record: LineageInputSourceRecord
) -> FoundInputBinding | None:
    binding = InputBinding(
        input_name=record.input_name,
        source=record.source,
        expects=record.expects,
        resolved=record.resolved,
        required=False,
        resolution=record.resolution,
    )
    if not resolved_payload_present(binding):
        return None
    return FoundInputBinding(
        source=record.source,
        resolved=record.resolved,
        resolution=replace(
            record.resolution,
            kind=record.resolution.kind or record.source.kind or "bound_input",
            resolved_from_lineage_root=str(root.resolve()),
        ),
    )


def _find_bound_input_resolution(
    root: Path,
    *,
    run_id: str,
    input_name: str,
    lineage_ref: str,
) -> FoundInputBinding | None:
    exact = find_bound_input(
        root, run_id=run_id, input_name=input_name, lineage_ref=lineage_ref
    )
    fallback = exact or find_bound_input(root, run_id=run_id, input_name=input_name)
    if fallback is not None:
        resolved = _record_resolution(root, fallback)
        if resolved is not None:
            return resolved
    for candidate in iter_lineage_source_roots(root):
        exact = find_bound_input(
            candidate, run_id=run_id, input_name=input_name, lineage_ref=lineage_ref
        )
        fallback = exact or find_bound_input(
            candidate, run_id=run_id, input_name=input_name
        )
        if fallback is None:
            continue
        resolved = _record_resolution(candidate, fallback)
        if resolved is not None:
            return resolved
    return None


def find_upstream_output(
    root: Path, lineage_ref: str, *, run_id: str, input_name: str
) -> FoundInputBinding | None:
    direct = _find_upstream_output_direct(root, lineage_ref)
    if direct is not None:
        return direct
    bound = _find_bound_input_resolution(
        root, run_id=run_id, input_name=input_name, lineage_ref=lineage_ref
    )
    if bound is not None:
        return bound
    for candidate in iter_lineage_source_roots(root):
        direct = _find_upstream_output_direct(candidate, lineage_ref)
        if direct is not None:
            return direct
    return None
