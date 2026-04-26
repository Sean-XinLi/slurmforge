from __future__ import annotations

from typing import Any

import yaml

from .errors import ConfigContractError


def deep_set(data: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cur = data
    for part in parts[:-1]:
        next_value = cur.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            cur[part] = next_value
        cur = next_value
    cur[parts[-1]] = value


def parse_override(item: str) -> tuple[str, Any]:
    if "=" not in item:
        raise ConfigContractError(f"Invalid --set syntax `{item}`. Expected key=value")
    key, raw = item.split("=", 1)
    key = key.strip()
    if not key:
        raise ConfigContractError(f"Invalid --set syntax `{item}`. Key is empty")
    return key, yaml.safe_load(raw)
