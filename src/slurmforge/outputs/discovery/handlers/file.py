from __future__ import annotations

from typing import Any

from ...artifact_store import manage_file
from ...models import output_ref_from_artifact
from ...selection import glob_paths, select_file
from ..context import OutputDiscoveryContext
from ..models import OutputDiscoveryItem


def discover_file_output(
    output_name: str,
    output_cfg: Any,
    context: OutputDiscoveryContext,
) -> OutputDiscoveryItem:
    paths = glob_paths(context.workdir, list(output_cfg.discover.globs))
    selected, reason = select_file(paths, output_cfg.discover.select)
    if not selected:
        return OutputDiscoveryItem(
            output_name=output_name,
            missing_required_reason=_missing_required(output_name)
            if output_cfg.required
            else "",
        )

    artifact = manage_file(
        selected,
        attempt_dir=context.attempt_dir,
        kind="file",
        output_name=output_name,
        optional=not output_cfg.required,
        store_plan=context.store_plan,
    )
    output_ref = output_ref_from_artifact(
        artifact,
        output_name=output_name,
        producer_stage_instance_id=context.instance.stage_instance_id,
        producer_attempt_id=context.attempt_id,
        selection_reason=reason,
    )
    return OutputDiscoveryItem(
        output_name=output_name, output_ref=output_ref, artifacts=(artifact,)
    )


def _missing_required(output_name: str) -> str:
    return f"required output `{output_name}` was not produced"
