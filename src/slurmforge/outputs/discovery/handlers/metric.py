from __future__ import annotations

import json

from ....contracts.outputs import StageOutputSpec
from ...artifact_store import manage_file
from ...selection import json_path_value, resolve_file
from ....errors import ConfigContractError
from ....plans.outputs import OutputRef
from ..context import OutputDiscoveryContext
from ..models import OutputDiscoveryItem
from .common import missing_required_output


def discover_metric_output(
    output_name: str,
    output_cfg: StageOutputSpec,
    context: OutputDiscoveryContext,
) -> OutputDiscoveryItem:
    metric_file = resolve_file(context.workdir, output_cfg.file).resolve()
    if not metric_file.exists() or not metric_file.is_file():
        return OutputDiscoveryItem(
            output_name=output_name,
            missing_required_reason=missing_required_output(output_name)
            if output_cfg.required
            else "",
        )

    try:
        with metric_file.open("r", encoding="utf-8") as handle:
            value = json_path_value(json.load(handle), output_cfg.json_path)
    except (
        OSError,
        ConfigContractError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as exc:
        reason = (
            f"required output `{output_name}` did not resolve: {exc}"
            if output_cfg.required
            else ""
        )
        return OutputDiscoveryItem(
            output_name=output_name, missing_required_reason=reason
        )

    artifact = manage_file(
        str(metric_file),
        attempt_dir=context.attempt_dir,
        kind="metric",
        output_name=output_name,
        optional=not output_cfg.required,
        store_plan=context.store_plan,
    )
    output_ref = OutputRef(
        output_name=output_name,
        kind="metric",
        path=artifact.managed_path,
        producer_stage_instance_id=context.instance.stage_instance_id,
        producer_attempt_id=context.attempt_id,
        digest=artifact.digest,
        source_path=artifact.source_path,
        managed=artifact.managed,
        strategy=artifact.strategy,
        source_digest=artifact.source_digest,
        managed_digest=artifact.managed_digest,
        verified=artifact.verified,
        size_bytes=artifact.size_bytes,
        value=value,
        selection_reason=f"json_path:{output_cfg.json_path}",
    )
    return OutputDiscoveryItem(
        output_name=output_name, output_ref=output_ref, artifacts=(artifact,)
    )
