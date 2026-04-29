from __future__ import annotations

from typing import Any

from ..config_contract.defaults import (
    DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
    DEFAULT_DISPATCH_OVERFLOW_POLICY,
)
from ..config_contract.options import options_for, options_sentence
from ..config_schema import reject_unknown_config_keys
from ..errors import ConfigContractError
from .models import DispatchSpec
from .parse_common import optional_mapping


def parse_dispatch(raw: Any) -> DispatchSpec:
    data = optional_mapping(raw, "dispatch")
    reject_unknown_config_keys(data, parent="dispatch")
    policy = str(data.get("overflow_policy") or DEFAULT_DISPATCH_OVERFLOW_POLICY)
    if policy not in options_for("dispatch.overflow_policy"):
        raise ConfigContractError(
            f"`dispatch.overflow_policy` must be {options_sentence('dispatch.overflow_policy')}"
        )
    return DispatchSpec(
        max_available_gpus=int(
            data.get("max_available_gpus", DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS)
            or DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS
        ),
        overflow_policy=policy,
    )
