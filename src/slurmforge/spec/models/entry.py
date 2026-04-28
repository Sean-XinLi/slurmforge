from __future__ import annotations

from dataclasses import dataclass, field

from .common import JsonObject


@dataclass(frozen=True)
class EntrySpec:
    type: str
    workdir: str
    args: JsonObject = field(default_factory=dict)
    script: str | None = None
    command: str | list[str] | None = None
