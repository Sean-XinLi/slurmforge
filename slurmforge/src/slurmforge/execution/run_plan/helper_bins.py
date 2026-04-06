from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


def resolve_runtime_helper_path(binary_name: str, env: dict[str, str]) -> str | None:
    search_dirs: list[Path] = []
    for candidate in (Path(sys.argv[0]).resolve().parent, Path(sys.executable).resolve().parent):
        if candidate not in search_dirs:
            search_dirs.append(candidate)
    existing_path = env.get("PATH", "")
    for raw in existing_path.split(os.pathsep):
        text = raw.strip()
        if not text:
            continue
        candidate = Path(text)
        if candidate not in search_dirs:
            search_dirs.append(candidate)
    search_path = os.pathsep.join(str(path) for path in search_dirs)
    if search_path:
        env["PATH"] = search_path
    return shutil.which(binary_name, path=search_path or None)
