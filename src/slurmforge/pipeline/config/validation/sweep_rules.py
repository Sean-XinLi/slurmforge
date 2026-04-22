from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError
from ....sweep import SweepSpec
from .definitions import (
    AUTHORING_SCHEMA,
    BATCH_SCOPED_SWEEP_EXACT_PATHS,
    BATCH_SCOPED_SWEEP_PREFIXES,
)
from .traversal import schema_without_fields, validate_override_path


def _path_matches_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}.")


def _matching_batch_scope(path: str) -> str | None:
    """Return the batch-scoped marker this path violates, or None if it's free.

    A path is batch-scoped when either:

    - it falls under one of ``BATCH_SCOPED_SWEEP_PREFIXES`` (whole-section rules
      like ``output`` / ``storage``), or
    - it is EXACTLY one of ``BATCH_SCOPED_SWEEP_EXACT_PATHS`` (fine-grained
      rules like ``resources.max_available_gpus`` where a sibling field such
      as ``resources.max_gpus_per_job`` remains sweepable).
    """
    for prefix in BATCH_SCOPED_SWEEP_PREFIXES:
        if _path_matches_prefix(path, prefix):
            return prefix
    for exact in BATCH_SCOPED_SWEEP_EXACT_PATHS:
        if path == exact:
            return exact
    return None


def validate_batch_scoped_sweep_paths(sweep_spec: Any, *, config_path: Path) -> None:
    violations: list[str] = []

    for path, _values in sweep_spec.shared_axes:
        marker = _matching_batch_scope(path)
        if marker is not None:
            violations.append(f"sweep.shared_axes.{path} (batch scope: {marker})")

    for idx, case in enumerate(sweep_spec.cases):
        for path, _value in case.set_values:
            marker = _matching_batch_scope(path)
            if marker is not None:
                violations.append(f"sweep.cases[{idx}].set.{path} (batch scope: {marker})")
        for path, _values in case.axes:
            marker = _matching_batch_scope(path)
            if marker is not None:
                violations.append(f"sweep.cases[{idx}].axes.{path} (batch scope: {marker})")

    if violations:
        protected = ", ".join(BATCH_SCOPED_SWEEP_PREFIXES + BATCH_SCOPED_SWEEP_EXACT_PATHS)
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
