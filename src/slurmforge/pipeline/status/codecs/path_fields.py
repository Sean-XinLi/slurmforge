from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError


def path_relative_to_base(base: str | Path | None, raw: str | None) -> str | None:
    if base in {None, ""} or raw in {None, ""}:
        return None
    base_path = Path(str(base)).expanduser()
    target_path = Path(str(raw)).expanduser()
    try:
        return target_path.resolve().relative_to(base_path.resolve()).as_posix()
    except ValueError:
        return None


def resolve_internal_result_path(
    *,
    current_result_dir: Path,
    rel_value: str | None,
    field_name: str,
) -> str | None:
    if rel_value in {None, ""}:
        return None
    candidate = Path(str(rel_value))
    if candidate.is_absolute():
        raise ConfigContractError(f"{field_name} must be relative, got absolute path: {candidate}")
    return str((current_result_dir / candidate).resolve())


def require_result_dir_context(result_dir: Path | None, *, payload_name: str) -> Path:
    if result_dir is None:
        raise ConfigContractError(f"{payload_name} requires result_dir context for path resolution")
    return result_dir.resolve()


def require_result_dir_rel(payload: dict[str, Any], *, payload_name: str) -> None:
    rel_value = payload.get("result_dir_rel")
    if rel_value in {None, ""}:
        raise ConfigContractError(f"{payload_name} is missing required result_dir_rel")
    candidate = Path(str(rel_value))
    if candidate.is_absolute():
        raise ConfigContractError(f"{payload_name}.result_dir_rel must be relative, got absolute path: {candidate}")
    if candidate.as_posix() != ".":
        raise ConfigContractError(f"{payload_name}.result_dir_rel must be '.'")


def reject_absolute_internal_fields(
    payload: dict[str, Any],
    *,
    payload_name: str,
    field_names: tuple[str, ...],
) -> None:
    present = [field for field in field_names if payload.get(field) not in {None, ""}]
    if present:
        raise ConfigContractError(f"{payload_name} must not persist absolute internal paths: {present}")
