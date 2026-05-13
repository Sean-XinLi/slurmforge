from __future__ import annotations

from ....contracts.outputs import StageOutputSpec
from ...artifact_store import manage_file
from ...selection import glob_paths
from ..context import OutputDiscoveryContext
from ..models import OutputDiscoveryItem
from ..writer import write_files_output_manifest
from .common import missing_required_output


def discover_files_output(
    output_name: str,
    output_cfg: StageOutputSpec,
    context: OutputDiscoveryContext,
) -> OutputDiscoveryItem:
    paths = glob_paths(context.workdir, list(output_cfg.discover.globs))
    if not paths:
        return OutputDiscoveryItem(
            output_name=output_name,
            missing_required_reason=missing_required_output(output_name)
            if output_cfg.required
            else "",
        )

    artifacts = tuple(
        manage_file(
            path,
            attempt_dir=context.attempt_dir,
            kind="files",
            output_name=output_name,
            optional=not output_cfg.required,
            store_plan=context.store_plan,
        )
        for path in paths
    )
    output_ref = write_files_output_manifest(output_name, artifacts, context)
    return OutputDiscoveryItem(
        output_name=output_name, output_ref=output_ref, artifacts=artifacts
    )
