from __future__ import annotations

from typing import Any

from ...config_contract.keys import reject_unknown_config_keys
from ...errors import ConfigContractError
from ..models import BeforeStepSpec
from ..parse_common import require_mapping


def parse_before(raw: Any, *, stage_name: str) -> tuple[BeforeStepSpec, ...]:
    if raw in (None, ""):
        return ()
    if not isinstance(raw, list):
        raise ConfigContractError(f"`stages.{stage_name}.before` must be a list")
    steps: list[BeforeStepSpec] = []
    for index, item in enumerate(raw):
        name = f"stages.{stage_name}.before[{index}]"
        data = require_mapping(item, name)
        reject_unknown_config_keys(data, parent=name)
        if data.get("run") in (None, ""):
            raise ConfigContractError(f"`{name}.run` is required")
        steps.append(
            BeforeStepSpec(
                name="" if data.get("name") in (None, "") else str(data.get("name")),
                run=str(data["run"]),
            )
        )
    return tuple(steps)
