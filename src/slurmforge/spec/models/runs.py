from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common import JsonObject


@dataclass(frozen=True)
class RunCaseSpec:
    name: str
    set: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class RunsSpec:
    type: str = "single"
    axes: tuple[tuple[str, tuple[Any, ...]], ...] = ()
    cases: tuple[RunCaseSpec, ...] = ()
