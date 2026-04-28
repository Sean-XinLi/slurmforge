from __future__ import annotations

import traceback
from pathlib import Path


def diagnostic_path(root: Path, *parts: str) -> Path:
    return Path(root).joinpath(*parts)


def write_exception_diagnostic(path: Path, exc: BaseException) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)), encoding="utf-8")
    return path.resolve()
