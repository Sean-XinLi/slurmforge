from __future__ import annotations

from typing import Any

from ..config_contract.keys import reject_unknown_config_keys
from ..config_contract.registry import default_for, options_for, options_sentence
from ..errors import ConfigContractError
from .models import DispatchSpec
from .parse_common import optional_mapping


def parse_dispatch(raw: Any) -> DispatchSpec:
    data = optional_mapping(raw, "dispatch")
    reject_unknown_config_keys(data, parent="dispatch")
    default_max_available_gpus = default_for("dispatch.max_available_gpus")
    policy = str(data.get("overflow_policy") or default_for("dispatch.overflow_policy"))
    if policy not in options_for("dispatch.overflow_policy"):
        raise ConfigContractError(
            f"`dispatch.overflow_policy` must be {options_sentence('dispatch.overflow_policy')}"
        )
    release_policy = str(
        data.get("release_policy") or default_for("dispatch.release_policy")
    )
    if release_policy not in options_for("dispatch.release_policy"):
        raise ConfigContractError(
            f"`dispatch.release_policy` must be {options_sentence('dispatch.release_policy')}"
        )
    return DispatchSpec(
        max_available_gpus=int(
            data.get("max_available_gpus", default_max_available_gpus)
            or default_max_available_gpus
        ),
        overflow_policy=policy,
        release_policy=release_policy,
        window_size=int(
            data.get("window_size", default_for("dispatch.window_size"))
            or default_for("dispatch.window_size")
        ),
        window_seconds=int(
            data.get("window_seconds", default_for("dispatch.window_seconds"))
            or default_for("dispatch.window_seconds")
        ),
    )
