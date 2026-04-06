from __future__ import annotations

import copy
import os
from pathlib import Path


def _canonicalize_replay_path(project_root: Path, raw: object) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return Path(os.path.relpath(path.resolve(), start=project_root.resolve())).as_posix()


def _canonicalize_replay_payload_path(payload: dict[str, object], path: tuple[str, ...], *, project_root: Path) -> None:
    node: object = payload
    for part in path[:-1]:
        if not isinstance(node, dict):
            return
        node = node.get(part)
        if node is None:
            return
    if not isinstance(node, dict):
        return
    key = path[-1]
    if key not in node:
        return
    canonical = _canonicalize_replay_path(project_root, node.get(key))
    if canonical is None:
        node.pop(key, None)
        return
    node[key] = canonical


def canonicalize_replay_payload(payload: dict[str, object], *, project_root: Path) -> dict[str, object]:
    replay_payload = copy.deepcopy(payload)
    path_fields = (
        ("model", "script"),
        ("model", "yaml"),
        ("run", "workdir"),
        ("run", "resume_from_checkpoint"),
        ("run", "adapter", "script"),
        ("run", "adapter", "workdir"),
        ("run", "adapter", "launcher", "workdir"),
        ("launcher", "workdir"),
        ("eval", "script"),
        ("eval", "workdir"),
        ("eval", "launcher", "workdir"),
        ("output", "base_output_dir"),
    )
    for field_path in path_fields:
        _canonicalize_replay_payload_path(replay_payload, field_path, project_root=project_root)
    return replay_payload
