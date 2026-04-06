from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def resolve_optional_path(project_root: Path, raw: Any) -> Path | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def canonicalize_optional_path(project_root: Path, raw: Any) -> str | None:
    resolved = resolve_optional_path(project_root, raw)
    if resolved is None:
        return None
    return Path(os.path.relpath(resolved, start=project_root.resolve())).as_posix()
