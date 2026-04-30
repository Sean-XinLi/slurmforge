from __future__ import annotations

from typing import Any

from ...config_contract.registry import default_for
from ...config_schema import reject_unknown_config_keys
from ..models import StageSpec
from ..parse_common import require_mapping
from ..parse_resources import parse_resources
from .before import parse_before
from .entry import parse_entry
from .gpu_sizing import parse_stage_gpu_sizing
from .inputs import parse_inputs
from .launcher import parse_launcher
from .outputs import parse_stage_output_config

DEFAULT_STAGE_ENABLED = default_for("stages.*.enabled")
DEFAULT_STAGE_ENVIRONMENT = default_for("stages.*.environment")
DEFAULT_STAGE_RUNTIME = default_for("stages.*.runtime")


def parse_stage(name: str, raw: Any) -> StageSpec:
    data = require_mapping(raw, f"stages.{name}")
    reject_unknown_config_keys(data, parent=f"stages.{name}")
    kind = str(data.get("kind") or name)
    depends_raw = data.get("depends_on") or ()
    if isinstance(depends_raw, str):
        depends = (depends_raw,)
    else:
        depends = tuple(str(item) for item in depends_raw)
    return StageSpec(
        name=name,
        kind=kind,
        enabled=bool(data.get("enabled", DEFAULT_STAGE_ENABLED)),
        depends_on=depends,
        entry=parse_entry(data.get("entry"), name=name),
        resources=parse_resources(data.get("resources"), name=name),
        launcher=parse_launcher(data.get("launcher"), name=name),
        runtime=str(data.get("runtime") or DEFAULT_STAGE_RUNTIME),
        environment=(
            DEFAULT_STAGE_ENVIRONMENT
            if data.get("environment") in (None, "")
            else str(data.get("environment"))
        ),
        gpu_sizing=parse_stage_gpu_sizing(data.get("gpu_sizing"), stage_name=name),
        before=parse_before(data.get("before"), stage_name=name),
        inputs=parse_inputs(data.get("inputs"), stage_name=name),
        outputs=parse_stage_output_config(data.get("outputs"), stage_name=name),
    )
