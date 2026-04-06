from __future__ import annotations

from typing import Any

import yaml

from ..errors import ConfigContractError


def deep_set(data: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cur: dict[str, Any] = data
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur[part], dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def parse_override(item: str) -> tuple[str, Any]:
    if "=" not in item:
        raise ConfigContractError(f"Invalid --set syntax `{item}`. Expected key=value")
    key, raw = item.split("=", 1)
    return key.strip(), yaml.safe_load(raw)
