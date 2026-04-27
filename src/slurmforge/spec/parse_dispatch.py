from __future__ import annotations

from typing import Any

from ..errors import ConfigContractError
from .models import DispatchSpec
from .parse_common import optional_mapping


def parse_dispatch(raw: Any) -> DispatchSpec:
    data = optional_mapping(raw, "dispatch")
    policy = str(data.get("overflow_policy") or "serialize_groups")
    if policy not in {"serialize_groups", "error", "best_effort"}:
        raise ConfigContractError("`dispatch.overflow_policy` must be serialize_groups, error, or best_effort")
    return DispatchSpec(
        max_available_gpus=int(data.get("max_available_gpus", 0) or 0),
        overflow_policy=policy,
    )
