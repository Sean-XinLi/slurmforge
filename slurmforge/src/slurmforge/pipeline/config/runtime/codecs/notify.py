from __future__ import annotations

from typing import Any

from ..models import NotifyConfig


def serialize_notify_config(config: NotifyConfig) -> dict[str, Any]:
    return {
        "enabled": bool(config.enabled),
        "email": str(config.email),
        "when": str(config.when),
    }
