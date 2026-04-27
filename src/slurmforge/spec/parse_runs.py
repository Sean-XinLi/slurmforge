from __future__ import annotations

import copy
from typing import Any

from ..errors import ConfigContractError
from .models import RunCaseSpec, RunsSpec
from .parse_common import optional_mapping, reject_unknown_keys, require_mapping


def parse_run_axes(raw: Any) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    axes = require_mapping(raw, "runs.axes")
    parsed: list[tuple[str, tuple[Any, ...]]] = []
    for key in sorted(axes):
        values = axes[key]
        if not isinstance(values, list) or not values:
            raise ConfigContractError(f"`runs.axes.{key}` must be a non-empty list")
        parsed.append((str(key), tuple(copy.deepcopy(values))))
    return tuple(parsed)


def parse_run_cases(raw: Any) -> tuple[RunCaseSpec, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigContractError("`runs.cases` must be a non-empty list")
    cases: list[RunCaseSpec] = []
    for index, item in enumerate(raw):
        name = f"runs.cases[{index}]"
        data = require_mapping(item, name)
        reject_unknown_keys(data, allowed={"name", "set"}, name=name)
        if data.get("name") in (None, ""):
            raise ConfigContractError(f"`{name}.name` is required")
        cases.append(
            RunCaseSpec(
                name=str(data["name"]),
                set=copy.deepcopy(optional_mapping(data.get("set"), f"{name}.set")),
            )
        )
    return tuple(cases)


def parse_runs(raw: Any) -> RunsSpec:
    if raw is None:
        return RunsSpec(type="single")
    data = require_mapping(raw, "runs")
    reject_unknown_keys(data, allowed={"type", "axes", "cases"}, name="runs")
    run_type = str(data.get("type") or "")
    if run_type not in {"single", "grid", "cases"}:
        raise ConfigContractError("`runs.type` must be single, grid, or cases")
    if run_type == "single":
        if "axes" in data or "cases" in data:
            raise ConfigContractError("`runs.type=single` cannot define axes or cases")
        return RunsSpec(type="single")
    if run_type == "grid":
        if "cases" in data:
            raise ConfigContractError("`runs.type=grid` cannot define cases")
        return RunsSpec(type="grid", axes=parse_run_axes(data.get("axes")))
    if "axes" in data:
        raise ConfigContractError("`runs.type=cases` cannot define axes")
    return RunsSpec(type="cases", cases=parse_run_cases(data.get("cases")))
