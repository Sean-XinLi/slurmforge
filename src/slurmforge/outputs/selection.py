from __future__ import annotations

import glob
import re
from pathlib import Path

from ..errors import ConfigContractError


def glob_paths(workdir: Path, patterns: list[str]) -> list[str]:
    paths: set[str] = set()
    for pattern in patterns:
        if not pattern:
            continue
        expanded = pattern if Path(pattern).is_absolute() else str(workdir / pattern)
        for match in glob.glob(expanded, recursive=True):
            if Path(match).is_file():
                paths.add(str(Path(match).resolve()))
    return sorted(paths)


def _step_number(path: str) -> int | None:
    numbers = [
        int(item)
        for item in re.findall(r"(?<![A-Za-z])(\d+)(?![A-Za-z])", Path(path).stem)
    ]
    return max(numbers) if numbers else None


def select_file(paths: list[str], selector: str) -> tuple[str | None, str]:
    if not paths:
        return None, "no_match"
    if selector == "latest_step":
        with_steps = [(path, _step_number(path)) for path in paths]
        if any(step is not None for _path, step in with_steps):
            selected = max(
                with_steps,
                key=lambda item: (-1 if item[1] is None else item[1], item[0]),
            )[0]
            return selected, "latest_step"
        return paths[-1], "lexicographic_last"
    if selector == "first":
        return paths[0], "first_match"
    return paths[-1], "last_match"


def resolve_file(workdir: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else workdir / path


def json_path_value(payload: object, path: str) -> object:
    if path == "$":
        return payload
    if not path.startswith("$."):
        raise ConfigContractError(f"unsupported metric json_path: {path}")
    cursor = payload
    for part in path[2:].split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            raise KeyError(f"json_path `{path}` did not resolve at `{part}`")
        cursor = cursor[part]
    return cursor
