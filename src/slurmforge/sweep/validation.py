from __future__ import annotations

from typing import Any

from ..errors import ConfigContractError
from .models import SweepCaseSpec, SweepSpec


def normalize_path(raw_key: Any, context_name: str) -> str:
    if not isinstance(raw_key, str):
        raise TypeError(f"{context_name} keys must be strings")
    key = raw_key.strip()
    if not key:
        raise ConfigContractError(f"{context_name} keys must be non-empty")
    parts = key.split(".")
    if any(not part.strip() for part in parts):
        raise ConfigContractError(f"{context_name} contains an invalid dot-path key: {raw_key!r}")
    return key


def paths_conflict(path_a: str, path_b: str) -> bool:
    return path_a == path_b or path_a.startswith(f"{path_b}.") or path_b.startswith(f"{path_a}.")


def ensure_no_internal_path_conflicts(keys: list[str], context_name: str) -> None:
    for idx, path_a in enumerate(keys):
        for path_b in keys[idx + 1:]:
            if paths_conflict(path_a, path_b):
                raise ConfigContractError(
                    f"{context_name} contains overlapping override paths: {path_a!r} and {path_b!r}"
                )


def ensure_no_cross_path_conflicts(
    left_keys: list[str],
    left_name: str,
    right_keys: list[str],
    right_name: str,
) -> None:
    for path_a in left_keys:
        for path_b in right_keys:
            if paths_conflict(path_a, path_b):
                raise ConfigContractError(
                    f"{left_name} and {right_name} contain overlapping override paths: "
                    f"{path_a!r} and {path_b!r}"
                )


def normalize_set_values(raw_values: Any, context_name: str) -> tuple[tuple[str, Any], ...]:
    if raw_values is None or raw_values == "":
        return ()
    if not isinstance(raw_values, dict):
        raise TypeError(f"{context_name} must be a mapping")

    normalized: dict[str, Any] = {}
    for raw_key, value in raw_values.items():
        key = normalize_path(raw_key, context_name)
        if key in normalized:
            raise ConfigContractError(f"{context_name} contains duplicate key {key!r}")
        normalized[key] = value

    keys = sorted(normalized.keys())
    ensure_no_internal_path_conflicts(keys, context_name)
    return tuple((key, normalized[key]) for key in keys)


def normalize_axes(raw_axes: Any, context_name: str) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    if raw_axes is None or raw_axes == "":
        return ()
    if not isinstance(raw_axes, dict):
        raise TypeError(f"{context_name} must be a mapping")

    normalized: dict[str, tuple[Any, ...]] = {}
    for raw_key, raw_values in raw_axes.items():
        key = normalize_path(raw_key, context_name)
        if key in normalized:
            raise ConfigContractError(f"{context_name} contains duplicate key {key!r}")
        if not isinstance(raw_values, list):
            raise TypeError(f"{context_name}.{key} must be a list")
        if not raw_values:
            raise ConfigContractError(f"{context_name}.{key} must not be empty")
        normalized[key] = tuple(raw_values)

    keys = sorted(normalized.keys())
    ensure_no_internal_path_conflicts(keys, context_name)
    return tuple((key, normalized[key]) for key in keys)


def normalize_case(raw_case: Any, idx: int) -> SweepCaseSpec:
    context_name = f"sweep.cases[{idx}]"
    if not isinstance(raw_case, dict):
        raise TypeError(f"{context_name} must be a mapping")

    allowed_keys = {"name", "set", "axes"}
    unknown_keys = sorted(str(key) for key in raw_case.keys() if key not in allowed_keys)
    if unknown_keys:
        raise ConfigContractError(f"{context_name} contains unsupported keys: {unknown_keys}")

    raw_name = raw_case.get("name")
    if not isinstance(raw_name, str) or not raw_name.strip():
        raise ConfigContractError(f"{context_name}.name must be a non-empty string")

    set_values = normalize_set_values(raw_case.get("set"), f"{context_name}.set")
    axes = normalize_axes(raw_case.get("axes"), f"{context_name}.axes")
    ensure_no_cross_path_conflicts(
        [key for key, _value in set_values],
        f"{context_name}.set",
        [key for key, _values in axes],
        f"{context_name}.axes",
    )
    return SweepCaseSpec(name=raw_name.strip(), set_values=set_values, axes=axes)


def max_runs_limit(sweep_cfg: dict[str, Any]) -> int | None:
    max_runs_raw = sweep_cfg.get("max_runs")
    if max_runs_raw is None:
        return None
    return max(0, int(max_runs_raw))


def normalize_sweep_config(cfg: dict[str, Any]) -> SweepSpec:
    sweep_cfg = cfg.get("sweep", {}) or {}
    if not isinstance(sweep_cfg, dict):
        raise TypeError("sweep must be a mapping when provided")

    allowed_keys = {"enabled", "max_runs", "shared_axes", "cases"}
    unknown_keys = sorted(str(key) for key in sweep_cfg.keys() if key not in allowed_keys)
    if unknown_keys:
        raise ConfigContractError(f"sweep contains unsupported keys: {unknown_keys}")

    enabled = bool(sweep_cfg.get("enabled", False))
    max_runs = max_runs_limit(sweep_cfg)
    shared_axes = normalize_axes(sweep_cfg.get("shared_axes"), "sweep.shared_axes")

    raw_cases = sweep_cfg.get("cases")
    if raw_cases is None or raw_cases == "":
        cases: tuple[SweepCaseSpec, ...] = ()
    else:
        if not isinstance(raw_cases, list):
            raise TypeError("sweep.cases must be a list")
        cases = tuple(normalize_case(raw_case, idx) for idx, raw_case in enumerate(raw_cases))

    if not shared_axes and not cases:
        if enabled:
            raise ConfigContractError("sweep requires at least one of sweep.shared_axes or sweep.cases when enabled=true")
        return SweepSpec(enabled=False, max_runs=max_runs, shared_axes=(), cases=())

    if not cases:
        cases = (SweepCaseSpec(name="default", set_values=(), axes=()),)

    shared_keys = [key for key, _values in shared_axes]
    for idx, case in enumerate(cases):
        ensure_no_cross_path_conflicts(
            shared_keys,
            "sweep.shared_axes",
            [key for key, _value in case.set_values],
            f"sweep.cases[{idx}].set",
        )
        ensure_no_cross_path_conflicts(
            shared_keys,
            "sweep.shared_axes",
            [key for key, _values in case.axes],
            f"sweep.cases[{idx}].axes",
        )

    return SweepSpec(
        enabled=enabled,
        max_runs=max_runs,
        shared_axes=shared_axes,
        cases=cases,
    )
