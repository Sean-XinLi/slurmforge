from __future__ import annotations

from pathlib import Path


def replace_between_markers(
    text: str,
    start_marker: str,
    end_marker: str,
    generated: str,
    *,
    path: Path,
) -> str:
    try:
        before, rest = text.split(start_marker, 1)
        _old, after = rest.split(end_marker, 1)
    except ValueError as exc:
        raise ValueError(f"{path} is missing generated content markers") from exc
    return f"{before}{start_marker}\n{generated}\n{end_marker}{after}"
