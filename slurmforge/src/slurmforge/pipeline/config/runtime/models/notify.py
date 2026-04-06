from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NotifyConfig:
    enabled: bool = False
    email: str = ""
    when: str = "afterany"
