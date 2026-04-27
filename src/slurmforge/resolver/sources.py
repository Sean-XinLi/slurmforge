from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..lineage import find_bound_input, iter_lineage_source_roots
from ..plans import RunDefinition
from ..schema import (
    InputBinding,
    InputSource,
    ResolvedInput,
    binding_is_ready_for_injection,
    input_source_from_dict,
    resolved_input_from_dict,
    resolved_payload_present,
)
from ..spec import ExperimentSpec
from ..storage.loader import iter_stage_run_dirs, load_stage_outputs, plan_for_run_dir
from .core import (
    inject_payload,
    output_ref,
    producer_root_from_run_dir,
    resolved_output,
    source_payload,
    unresolved_resolved,
    upstream_resolution,
)


def _output_resolution(
    *,
    root: Path,
    run_dir: Path,
    lineage_ref: str,
    output_name: str,
    output: dict,
    producer_stage_instance_id: str,
    producer_run_id: str,
    producer_stage_name: str,
) -> dict:
    resolution = upstream_resolution(
        producer_root=producer_root_from_run_dir(run_dir),
        run_dir=run_dir,
        stage_instance_id=producer_stage_instance_id,
        run_id=producer_run_id,
        stage_name=producer_stage_name,
        output_name=output_name,
        output=output,
    )
    resolution["searched_root"] = str(root.resolve())
    return {
        "source": InputSource(kind="upstream_output", stage=producer_stage_name, output=output_name),
        "lineage_ref": lineage_ref,
        "resolved": resolved_output(output),
        "resolution": resolution,
    }


def _find_upstream_output_direct(root: Path, lineage_ref: str) -> dict | None:
    if ":" not in lineage_ref:
        return None
    producer, output_name = lineage_ref.split(":", 1)
    try:
        run_dirs = tuple(iter_stage_run_dirs(root))
    except FileNotFoundError:
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


def _record_resolution(root: Path, record: dict) -> dict | None:
    resolved = resolved_input_from_dict(record.get("resolved"))
    binding = InputBinding(
        input_name=str(record.get("input_name") or ""),
        source=input_source_from_dict(dict(record.get("source") or {"kind": "upstream_output"})),
        expects=str(record.get("expects") or resolved.kind),
        resolved=resolved,
    )
    if not resolved_payload_present(binding):
        return None
    resolution = dict(record.get("resolution") or {})
    source = input_source_from_dict(dict(record.get("source") or {"kind": "upstream_output"}))
    resolution.setdefault("kind", source.kind or "bound_input")
    resolution["resolved_from_lineage_root"] = str(root.resolve())
    return {
        "source": source,
        "resolved": resolved,
        "resolution": resolution,
    }


def _find_bound_input_resolution(
    root: Path,
    *,
    run_id: str,
    input_name: str,
    lineage_ref: str,
) -> dict | None:
    exact = find_bound_input(root, run_id=run_id, input_name=input_name, lineage_ref=lineage_ref)
    fallback = exact or find_bound_input(root, run_id=run_id, input_name=input_name)
    if fallback is not None:
        resolved = _record_resolution(root, fallback)
        if resolved is not None:
            return resolved
    for candidate in iter_lineage_source_roots(root):
        exact = find_bound_input(candidate, run_id=run_id, input_name=input_name, lineage_ref=lineage_ref)
        fallback = exact or find_bound_input(candidate, run_id=run_id, input_name=input_name)
        if fallback is None:
            continue
        resolved = _record_resolution(candidate, fallback)
        if resolved is not None:
            return resolved
    return None


def _find_upstream_output(root: Path, lineage_ref: str, *, run_id: str, input_name: str) -> dict | None:
    direct = _find_upstream_output_direct(root, lineage_ref)
    if direct is not None:
        return direct
    bound = _find_bound_input_resolution(root, run_id=run_id, input_name=input_name, lineage_ref=lineage_ref)
    if bound is not None:
        return bound
    for candidate in iter_lineage_source_roots(root):
        direct = _find_upstream_output_direct(candidate, lineage_ref)
        if direct is not None:
            return direct
    return None


def resolve_stage_inputs_from_prior_source(
    *,
    spec: ExperimentSpec,
    source_root: Path,
    stage_name: str,
    run: RunDefinition,
) -> tuple[InputBinding, ...]:
    stage = spec.enabled_stages[stage_name]
    bindings: list[InputBinding] = []
    for input_name in sorted(stage.inputs):
        input_spec = stage.inputs[input_name]
        source = source_payload(input_spec)
        resolved_payload = unresolved_resolved()
        resolution = {"kind": source.kind, "state": "unresolved"}
        if source.kind == "upstream_output":
            upstream_stage, output_name = source.stage, source.output
            lineage_ref = f"{run.run_id}.{upstream_stage}:{output_name}"
            resolved = _find_upstream_output(
                source_root,
                lineage_ref,
                run_id=run.run_id,
                input_name=input_name,
            )
            if resolved is not None:
                source = input_source_from_dict(resolved["source"])
                resolved_payload = resolved_input_from_dict(resolved.get("resolved"))
                resolution = dict(resolved["resolution"])
        elif source.kind == "external_path":
            source_path = Path(source.path).expanduser()
            source_path = source_path if source_path.is_absolute() else spec.project_root / source_path
            source_path = source_path.resolve()
            resolved_payload = (
                ResolvedInput(kind=input_spec.expects, path=str(source_path))
                if source_path.exists()
                else unresolved_resolved(path=str(source_path))
            )
            resolution = {
                "kind": source.kind,
                "resolved": {"kind": resolved_payload.kind, "path": resolved_payload.path},
                "source_exists": source_path.exists(),
            }
        inject = inject_payload(input_spec)
        binding = InputBinding(
            input_name=input_spec.name,
            source=source,
            expects=input_spec.expects,
            resolved=resolved_payload,
            inject=inject,
            resolution=resolution,
        )
        if input_spec.required and not binding_is_ready_for_injection(binding):
            raise ConfigContractError(
                f"Cannot bind `{input_name}` for run `{run.run_id}` from prior source: "
                "source did not resolve for injection"
            )
        bindings.append(binding)
    return tuple(bindings)
