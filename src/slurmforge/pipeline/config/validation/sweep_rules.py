from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError
from ....sweep import SweepSpec
from ..contracts import FieldContract, contract_for_path
from .definitions import AUTHORING_SCHEMA
from .traversal import schema_without_fields, validate_override_path


def _violation_line(location: str, path: str, contract: FieldContract | None) -> str:
    """Render one violation row for the aggregate error message.

    The row names both where in the sweep block the offense occurred and
    *why* the field is off-limits, so the user can fix the config in one
    pass rather than bouncing between errors.
    """
    if contract is None:
        return f"{location} (unregistered config path)"
    reason = (
        f"{contract.lifecycle}-scoped"
        if contract.lifecycle == "batch"
        else f"lifecycle={contract.lifecycle}, source={contract.source}"
    )
    return f"{location} ({reason})"


def _remedy_hint(path: str, contract: FieldContract | None) -> str:
    """Per-field remedy string appended to the aggregate error."""
    if contract is None:
        return f"  - {path}: path not registered in field contracts"
    if contract.lifecycle == "batch":
        return (
            f"  - {path}: batch-scoped; set it once at top-level {path} "
            f"(every run in the batch must agree)"
        )
    if contract.lifecycle == "meta" and contract.source == "authoring_only":
        section = path.split(".", 1)[0]
        return (
            f"  - {path}: authoring source field; set {section} once at "
            f"top-level (not per run)"
        )
    return f"  - {path}: cannot be overridden by sweep"


def _collect_violations(sweep_spec: SweepSpec) -> list[tuple[str, str, FieldContract | None]]:
    """Return ``(location, path, contract)`` triples for every disallowed sweep axis.

    Reads each axis path against the authoring-source view of the registry.
    ``contract is None`` flags an unregistered path.  Otherwise
    ``not contract.sweep_allowed`` selects entries that must be rejected.
    """
    violations: list[tuple[str, str, FieldContract | None]] = []

    def _check(location: str, path: str) -> None:
        contract = contract_for_path(path, source="authoring")
        if contract is None or not contract.sweep_allowed:
            violations.append((location, path, contract))

    for path, _values in sweep_spec.shared_axes:
        _check(f"sweep.shared_axes.{path}", path)

    for idx, case in enumerate(sweep_spec.cases):
        for path, _value in case.set_values:
            _check(f"sweep.cases[{idx}].set.{path}", path)
        for path, _values in case.axes:
            _check(f"sweep.cases[{idx}].axes.{path}", path)

    return violations


def validate_batch_scoped_sweep_paths(sweep_spec: Any, *, config_path: Path) -> None:
    violations = _collect_violations(sweep_spec)
    if not violations:
        return

    # Deduplicate paths for the remedy block — the aggregate message should
    # name each offending field once, regardless of how many places in the
    # sweep block referenced it.
    seen_paths: dict[str, FieldContract | None] = {}
    lines: list[str] = []
    for location, path, contract in violations:
        lines.append(_violation_line(location, path, contract))
        if path not in seen_paths:
            seen_paths[path] = contract

    details = "; ".join(lines)
    remedies = "\n".join(_remedy_hint(p, c) for p, c in seen_paths.items())
    raise ConfigContractError(
        f"{config_path}: sweep cannot override these fields:\n"
        f"{remedies}\n"
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
