from __future__ import annotations

import re

from ..errors import ConfigContractError
from .models import ExperimentSpec
from .validation_common import path_exists_or_allowed_for_args


def validate_runs_contract(spec: ExperimentSpec) -> None:
    if spec.runs.type == "single":
        return
    if spec.runs.type == "grid":
        if not spec.runs.axes:
            raise ConfigContractError("`runs.axes` must contain at least one axis for grid runs")
        for path, _values in spec.runs.axes:
            if not path_exists_or_allowed_for_args(spec.raw, path):
                raise ConfigContractError(f"`runs.axes.{path}` does not target a known config path")
        return
    if spec.runs.type == "cases":
        if not spec.runs.cases:
            raise ConfigContractError("`runs.cases` must contain at least one case")
        seen: set[str] = set()
        for case in spec.runs.cases:
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", case.name):
                raise ConfigContractError(
                    "`runs.cases[].name` may only contain letters, numbers, underscores, dots, and dashes"
                )
            if case.name in seen:
                raise ConfigContractError(f"`runs.cases[].name` must be unique: {case.name}")
            seen.add(case.name)
            for path in case.set:
                if not path_exists_or_allowed_for_args(spec.raw, path):
                    raise ConfigContractError(f"`runs.cases.{case.name}.set.{path}` does not target a known config path")
        return
    raise ConfigContractError("`runs.type` must be single, grid, or cases")
