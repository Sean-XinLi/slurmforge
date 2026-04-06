from __future__ import annotations

from pathlib import Path
from typing import Any
import warnings

from ...errors import ConfigContractError


def _warn(message: str) -> None:
    warnings.warn(message, stacklevel=2)

def ensure_dict(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigContractError(f"{name} must be a mapping")
    return value


def resolve_path(project_root: Path, raw: str | None, default: str) -> Path:
    candidate = Path(raw or default).expanduser()
    if not candidate.is_absolute():
        candidate = (project_root / candidate).resolve()
    return candidate


def resolve_spec_project_root(config_path: Path | None, project_root: Path | None) -> Path:
    if project_root is not None:
        return project_root.expanduser().resolve()
    if config_path is None:
        raise ConfigContractError("project_root is required when config_path is not available")
    return config_path.expanduser().resolve().parent


def resolve_config_label(
    *,
    config_path: Path | None,
    config_label: str | None,
    default: str,
) -> str:
    if config_label is not None:
        label = str(config_label).strip()
        if label:
            return label
    if config_path is not None:
        return str(config_path)
    return default


def _is_auto_value(value: Any) -> bool:
    return value is None or value == "" or (isinstance(value, str) and value.strip().lower() == "auto")


def non_empty_text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def ensure_path_segment(value: Any, *, name: str) -> str:
    text = non_empty_text(value)
    if text is None:
        raise ConfigContractError(f"{name} must be a non-empty string")
    if text in {".", ".."} or "/" in text or "\\" in text:
        raise ConfigContractError(f"{name} must be a single path segment without separators or traversal")
    return text
