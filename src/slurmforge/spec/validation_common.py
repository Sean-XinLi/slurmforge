from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from .models import ExperimentSpec, StageSpec
from .queries import normalize_run_path


def resolve_workdir(spec: ExperimentSpec, stage: StageSpec) -> Path:
    workdir = Path(stage.entry.workdir)
    return workdir if workdir.is_absolute() else spec.project_root / workdir


def resolve_script(spec: ExperimentSpec, stage: StageSpec) -> Path:
    script = Path(str(stage.entry.script))
    if script.is_absolute():
        return script
    return resolve_workdir(spec, stage) / script


def path_exists_or_allowed_for_args(raw: dict[str, Any], path: str) -> bool:
    normalized = normalize_run_path(raw, path)
    parts = normalized.split(".")
    if len(parts) >= 5 and parts[:2] == ["stages", parts[1]] and parts[2:4] == ["entry", "args"]:
        return parts[1] in {"train", "eval"}
    cursor: Any = raw
    for part in parts:
        if not isinstance(cursor, dict) or part not in cursor:
            return False
        cursor = cursor[part]
    return True


def explicit_int(raw: Any, *, field: str) -> int | None:
    if raw in (None, "", "auto"):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigContractError(f"`{field}` must be an integer or auto") from exc


def require_port(raw: Any, *, field: str) -> int:
    value = explicit_int(raw, field=field)
    if value is None:
        value = 29500
    if value < 1 or value > 65535:
        raise ConfigContractError(f"`{field}` must be between 1 and 65535")
    return value


def reject_newline(value: str, *, field: str) -> None:
    if "\n" in value:
        raise ConfigContractError(f"`{field}` cannot contain newlines")
