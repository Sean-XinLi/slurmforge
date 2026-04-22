from __future__ import annotations

from typing import Any

from ..models import DispatchConfig


def serialize_dispatch_config(config: DispatchConfig) -> dict[str, Any]:
    return {
        "group_overflow_policy": str(config.group_overflow_policy),
    }
