from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ...contracts import InputBinding
from ...io import file_digest


@dataclass(frozen=True)
class PathVerification:
    path: Path
    path_kind: str
    exists: bool
    is_valid_kind: bool
    readable: bool
    size_bytes: int | None
    digest: str = ""


def path_kind_for_binding(binding: InputBinding) -> str:
    resolution = dict(binding.resolution or {})
    if binding.resolved.kind == "manifest":
        return "file"
    return str(resolution.get("path_kind") or "file")


def verify_resolved_path(binding: InputBinding, *, expected_digest: str) -> PathVerification:
    path_kind = path_kind_for_binding(binding)
    path = Path(binding.resolved.path).expanduser()
    exists = path.exists()
    is_valid_kind = exists and (path.is_file() if path_kind == "file" else path.is_dir() if path_kind == "dir" else True)
    readable = is_valid_kind and os.access(path, os.R_OK)
    size_bytes = path.stat().st_size if is_valid_kind and path.is_file() else None
    digest = file_digest(path) if expected_digest and is_valid_kind and path.is_file() else ""
    return PathVerification(
        path=path,
        path_kind=path_kind,
        exists=exists,
        is_valid_kind=is_valid_kind,
        readable=readable,
        size_bytes=size_bytes,
        digest=digest,
    )
