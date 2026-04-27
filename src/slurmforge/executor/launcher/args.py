from __future__ import annotations

from typing import Any


def flag(name: str) -> str:
    return name if name.startswith("-") else f"--{name}"


def args_to_argv(args: dict[str, Any]) -> list[str]:
    argv: list[str] = []
    for key in sorted(args):
        value = args[key]
        if value is None:
            continue
        item_flag = flag(str(key).replace("_", "-"))
        if isinstance(value, bool):
            argv.append(item_flag)
            if not value:
                argv.append("false")
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                argv.extend([item_flag, str(item)])
            continue
        argv.extend([item_flag, str(value)])
    return argv
