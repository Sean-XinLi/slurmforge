from __future__ import annotations

from typing import Any

from ...contracts.outputs import parse_stage_output_contract
from ..models import StageSpec
from ..parse_common import reject_unknown_keys, require_mapping
from ..parse_resources import parse_resources
from .before import parse_before
from .entry import parse_entry
from .gpu_sizing import parse_stage_gpu_sizing
from .inputs import parse_inputs
from .launcher import parse_launcher


def parse_stage(name: str, raw: Any) -> StageSpec:
    data = require_mapping(raw, f"stages.{name}")
    reject_unknown_keys(
        data,
        allowed={
            "enabled",
            "kind",
            "depends_on",
            "entry",
            "resources",
            "launcher",
            "runtime",
            "environment",
            "gpu_sizing",
            "before",
            "inputs",
            "outputs",
        },
        name=f"stages.{name}",
    )
    kind = str(data.get("kind") or name)
    depends_raw = data.get("depends_on") or ()
    if isinstance(depends_raw, str):
        depends = (depends_raw,)
    else:
        depends = tuple(str(item) for item in depends_raw)
    return StageSpec(
        name=name,
        kind=kind,
        enabled=bool(data.get("enabled", True)),
        depends_on=depends,
        entry=parse_entry(data.get("entry"), name=name),
        resources=parse_resources(data.get("resources"), name=name),
        launcher=parse_launcher(data.get("launcher"), name=name),
        runtime=str(data.get("runtime") or "default"),
        environment="" if data.get("environment") in (None, "") else str(data.get("environment")),
        gpu_sizing=parse_stage_gpu_sizing(data.get("gpu_sizing"), stage_name=name),
        before=parse_before(data.get("before"), stage_name=name),
        inputs=parse_inputs(data.get("inputs"), stage_name=name),
        outputs=parse_stage_output_contract(data.get("outputs"), stage_name=name),
    )
