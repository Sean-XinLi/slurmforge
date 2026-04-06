from __future__ import annotations

from typing import Any, Callable, TypeVar

from ..utils import ensure_dict

T = TypeVar("T")


def ensure_normalized_config(
    value: Any,
    *,
    name: str,
    config_type: type[T],
    normalizer: Callable[[dict[str, Any]], T],
) -> T:
    if isinstance(value, config_type):
        return value
    return normalizer(ensure_dict(value, name))
