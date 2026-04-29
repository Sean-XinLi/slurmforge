from __future__ import annotations

from pathlib import Path

from ..contracts import ResolvedInput, resolved_input_from_output_ref
from ..errors import ConfigContractError
from ..spec import StageInputSpec


def output_ref(payload: dict, output_name: str) -> dict | None:
    outputs = dict(payload.get("outputs") or {})
    output = outputs.get(output_name)
    if isinstance(output, dict) and output.get("path"):
        return dict(output)
    return None


def producer_root_from_run_dir(run_dir: Path) -> Path:
    if run_dir.parent.name == "runs":
        return run_dir.parent.parent.resolve()
    return run_dir.parent.resolve()


def resolved_output(output: dict) -> ResolvedInput:
    return resolved_input_from_output_ref(output)


def upstream_resolution(
    *,
    producer_root: Path,
    run_dir: Path,
    stage_instance_id: str,
    run_id: str,
    stage_name: str,
    output_name: str,
    output: dict,
) -> dict[str, object]:
    return {
        "kind": "upstream_output",
        "producer_root": str(producer_root.resolve()),
        "producer_run_dir": str(run_dir.resolve()),
        "producer_stage_instance_id": stage_instance_id,
        "producer_run_id": run_id,
        "producer_stage_name": stage_name,
        "output_name": output_name,
        "output_path": str(output["path"]),
        "output_digest": str(
            output.get("digest") or output.get("managed_digest") or ""
        ),
        "selection_reason": str(output.get("selection_reason") or ""),
    }


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
