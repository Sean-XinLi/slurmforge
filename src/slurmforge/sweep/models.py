from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SweepCaseSpec:
    name: str
    set_values: tuple[tuple[str, Any], ...]
    axes: tuple[tuple[str, tuple[Any, ...]], ...]


@dataclass(frozen=True)
class SweepSpec:
    enabled: bool
    max_runs: int | None = None
    shared_axes: tuple[tuple[str, tuple[Any, ...]], ...] = ()
    cases: tuple[SweepCaseSpec, ...] = ()


@dataclass(frozen=True)
class SweepExpansion:
    case_name: str | None
    assignments: tuple[tuple[str, Any], ...] = ()
