from __future__ import annotations

from pathlib import Path
from typing import Any

from .....errors import ConfigContractError
from ...utils import _warn


def normalize_optional_text(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigContractError(f"{name} must be a string when provided")
    text = value.strip()
    return text or None


def warn_external_command_raw_shell_semantics(
    *,
    config_path: Path | str,
    mode_field_name: str,
    command_field_name: str,
) -> None:
    _warn(
        f"{config_path}: {mode_field_name}=raw executes {command_field_name} as raw shell text. "
        "Shell expansions and operators such as $, $(...), backticks, redirections, and globs will be interpreted by bash. "
        f"Use {mode_field_name}=argv if you want argument-safe command rendering."
    )
