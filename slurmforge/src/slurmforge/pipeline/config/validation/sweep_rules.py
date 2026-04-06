from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError
from ....sweep import SweepSpec
from .definitions import AUTHORING_SCHEMA, BATCH_SCOPED_SWEEP_PREFIXES
from .traversal import schema_without_fields, validate_override_path


def _path_matches_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}.")


def _matching_batch_scoped_prefix(path: str) -> str | None:
    for prefix in BATCH_SCOPED_SWEEP_PREFIXES:
        if _path_matches_prefix(path, prefix):
            return prefix
    return None


def validate_batch_scoped_sweep_paths(sweep_spec: Any, *, config_path: Path) -> None:
    violations: list[str] = []

    for path, _values in sweep_spec.shared_axes:
        matched_prefix = _matching_batch_scoped_prefix(path)
        if matched_prefix is not None:
            violations.append(f"sweep.shared_axes.{path} (batch scope: {matched_prefix})")

    for idx, case in enumerate(sweep_spec.cases):
        for path, _value in case.set_values:
            matched_prefix = _matching_batch_scoped_prefix(path)
            if matched_prefix is not None:
                violations.append(f"sweep.cases[{idx}].set.{path} (batch scope: {matched_prefix})")
        for path, _values in case.axes:
            matched_prefix = _matching_batch_scoped_prefix(path)
            if matched_prefix is not None:
                violations.append(f"sweep.cases[{idx}].axes.{path} (batch scope: {matched_prefix})")

    if violations:
        protected = ", ".join(BATCH_SCOPED_SWEEP_PREFIXES)
        details = "; ".join(violations)
        raise ConfigContractError(
            f"{config_path}: sweep cannot override batch-scoped fields. "
            f"Keep these constant for the whole batch: {protected}. "
            f"Found: {details}"
        )


def validate_declared_sweep_paths(sweep_spec: SweepSpec, *, config_path: Path) -> None:
    override_schema = schema_without_fields(AUTHORING_SCHEMA, "sweep")
    for path, _values in sweep_spec.shared_axes:
        validate_override_path(
            path,
            context_name=f"{config_path}: sweep.shared_axes",
            schema=override_schema,
        )

    for idx, case in enumerate(sweep_spec.cases):
        for path, _value in case.set_values:
            validate_override_path(
                path,
                context_name=f"{config_path}: sweep.cases[{idx}].set",
                schema=override_schema,
            )
        for path, _values in case.axes:
            validate_override_path(
                path,
                context_name=f"{config_path}: sweep.cases[{idx}].axes",
                schema=override_schema,
            )
