from __future__ import annotations

import shlex
from typing import Any

from ....errors import ConfigContractError


def q(value: Any) -> str:
    return shlex.quote(str(value))


def sanitize_activate_command(field_name: str, value: str) -> str | None:
    command = value.strip()
    if not command:
        return None
    unsafe_tokens = [";", "&&", "&", "||", "`", "$(", "${", "|", ">", "<", "\n", "\r"]
    for token in unsafe_tokens:
        if token in command:
            raise ConfigContractError(f"{field_name} contains unsafe shell token `{token}`")
    return command


def emit(lines: list[str], message: str) -> None:
    lines.append(f"printf '%s\\n' {q(message)}")
