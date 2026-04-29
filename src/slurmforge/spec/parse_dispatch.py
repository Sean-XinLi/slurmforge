from __future__ import annotations

from typing import Any

from ..errors import ConfigContractError
from ..config_schema import options_for, options_sentence
from .models import DispatchSpec
from .parse_common import optional_mapping


def parse_dispatch(raw: Any) -> DispatchSpec:
    data = optional_mapping(raw, "dispatch")
    policy = str(data.get("overflow_policy") or "serialize_groups")
    if policy not in options_for("dispatch.overflow_policy"):
        raise ConfigContractError(
            f"`dispatch.overflow_policy` must be {options_sentence('dispatch.overflow_policy')}"
        )
    return DispatchSpec(
        max_available_gpus=int(data.get("max_available_gpus", 0) or 0),
        overflow_policy=policy,
    )
