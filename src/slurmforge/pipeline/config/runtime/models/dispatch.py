from __future__ import annotations

from dataclasses import dataclass


GROUP_OVERFLOW_POLICIES: tuple[str, ...] = ("error", "serial", "best_effort")


@dataclass(frozen=True)
class DispatchConfig:
    group_overflow_policy: str = "error"
