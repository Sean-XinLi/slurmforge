from __future__ import annotations

import copy
from typing import Any

from ..errors import ConfigContractError
from ..config_schema import options_for, options_sentence
from .models import RunVariantSpec, RunsSpec
from .parse_common import optional_mapping, reject_unknown_keys, require_mapping


def parse_run_axes(
    raw: Any, *, name: str = "runs.axes"
) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    axes = require_mapping(raw, name)
    parsed: list[tuple[str, tuple[Any, ...]]] = []
    for key in sorted(axes):
        values = axes[key]
        if not isinstance(values, list) or not values:
            raise ConfigContractError(f"`{name}.{key}` must be a non-empty list")
        parsed.append((str(key), tuple(copy.deepcopy(values))))
    return tuple(parsed)


def parse_run_cases(
    raw: Any, *, allow_axes: bool = False
) -> tuple[RunVariantSpec, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigContractError("`runs.cases` must be a non-empty list")
    cases: list[RunVariantSpec] = []
    for index, item in enumerate(raw):
        name = f"runs.cases[{index}]"
        data = require_mapping(item, name)
        allowed = {"name", "set", "axes"} if allow_axes else {"name", "set"}
        reject_unknown_keys(data, allowed=allowed, name=name)
        if data.get("name") in (None, ""):
            raise ConfigContractError(f"`{name}.name` is required")
        case_name = str(data["name"])
        cases.append(
            RunVariantSpec(
                name=case_name,
                set=copy.deepcopy(optional_mapping(data.get("set"), f"{name}.set")),
                axes=(
                    parse_run_axes(data.get("axes"), name=f"{name}.axes")
                    if allow_axes and "axes" in data
                    else ()
                ),
            )
        )
    return tuple(cases)


def parse_runs(raw: Any) -> RunsSpec:
    if raw is None:
        return RunsSpec(type="single")
    data = require_mapping(raw, "runs")
    reject_unknown_keys(data, allowed={"type", "axes", "cases"}, name="runs")
    run_type = str(data.get("type") or "")
    if run_type not in options_for("runs.type"):
        raise ConfigContractError(
            f"`runs.type` must be {options_sentence('runs.type')}"
        )
    if run_type == "single":
        if "axes" in data or "cases" in data:
            raise ConfigContractError("`runs.type=single` cannot define axes or cases")
        return RunsSpec(type="single")
    if run_type == "grid":
        if "cases" in data:
            raise ConfigContractError("`runs.type=grid` cannot define cases")
        return RunsSpec(type="grid", axes=parse_run_axes(data.get("axes")))
    if run_type == "cases":
        if "axes" in data:
            raise ConfigContractError("`runs.type=cases` cannot define axes")
        return RunsSpec(type="cases", cases=parse_run_cases(data.get("cases")))
    if "axes" in data:
        raise ConfigContractError(
            "`runs.type=matrix` defines axes under each case, not top-level `runs.axes`"
        )
    if "cases" not in data:
        raise ConfigContractError("`runs.cases` must contain at least one case")
    return RunsSpec(
        type="matrix",
        cases=parse_run_cases(data.get("cases"), allow_axes=True),
    )
