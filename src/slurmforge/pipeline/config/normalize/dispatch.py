from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from ...utils import deep_merge
from ..runtime import DEFAULT_DISPATCH, DispatchConfig
from ..runtime.models.dispatch import GROUP_OVERFLOW_POLICIES
from ..utils import ensure_dict
from .shared import ensure_normalized_config


def normalize_dispatch(cfg: dict[str, Any]) -> DispatchConfig:
    merged = deep_merge(DEFAULT_DISPATCH, ensure_dict(cfg, "dispatch"))

    policy_raw = merged.get("group_overflow_policy", "error")
    if not isinstance(policy_raw, str):
        raise ConfigContractError("dispatch.group_overflow_policy must be a string")
    policy = policy_raw.strip().lower()
    if policy not in GROUP_OVERFLOW_POLICIES:
        allowed = ", ".join(GROUP_OVERFLOW_POLICIES)
        raise ConfigContractError(
            f"dispatch.group_overflow_policy must be one of: {allowed} (got {policy_raw!r})"
        )

    return DispatchConfig(group_overflow_policy=policy)


def ensure_dispatch_config(value: Any, name: str = "dispatch") -> DispatchConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=DispatchConfig,
        normalizer=normalize_dispatch,
    )
