from __future__ import annotations

from typing import Any

from ...artifact_store import manage_file
from ...models import output_ref_from_artifact
from ...selection import resolve_file
from ..context import OutputDiscoveryContext
from ..models import OutputDiscoveryItem


def discover_manifest_output(
    output_name: str,
    output_cfg: Any,
    context: OutputDiscoveryContext,
) -> OutputDiscoveryItem:
    manifest_file = resolve_file(context.workdir, output_cfg.file).resolve()
    if not manifest_file.exists() or not manifest_file.is_file():
        return OutputDiscoveryItem(
            output_name=output_name,
            missing_required_reason=_missing_required(output_name) if output_cfg.required else "",
        )

    artifact = manage_file(
        str(manifest_file),
        attempt_dir=context.attempt_dir,
        kind="manifest",
        output_name=output_name,
        optional=not output_cfg.required,
        store_plan=context.store_plan,
    )
    output_ref = output_ref_from_artifact(
        artifact,
        output_name=output_name,
        producer_stage_instance_id=context.instance.stage_instance_id,
        producer_attempt_id=context.attempt_id,
        selection_reason="manifest_file",
    )
    return OutputDiscoveryItem(output_name=output_name, output_ref=output_ref, artifacts=(artifact,))


def _missing_required(output_name: str) -> str:
    return f"required output `{output_name}` was not produced"
