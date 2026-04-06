from __future__ import annotations

import re
from pathlib import Path

_STEP_PATTERNS = (
    re.compile(r"global[_-]?step[_=-]?(\d+)", re.IGNORECASE),
    re.compile(r"step[_=-]?(\d+)", re.IGNORECASE),
    re.compile(r"checkpoint[_=-]?(\d+)", re.IGNORECASE),
    re.compile(r"ckpt[_=-]?(\d+)", re.IGNORECASE),
)


def discover_checkpoint_files(result_dir: Path, checkpoint_globs: list[str] | tuple[str, ...]) -> list[Path]:
    discovered: dict[Path, Path] = {}
    checkpoint_root = result_dir / "checkpoints"
    if checkpoint_root.exists():
        for path in checkpoint_root.rglob("*"):
            if path.is_file():
                discovered[path.resolve()] = path.resolve()

    for pattern in checkpoint_globs:
        for path in result_dir.glob(pattern):
            if path.is_file():
                discovered[path.resolve()] = path.resolve()

    return sorted(discovered.values())


def extract_checkpoint_step(path: Path) -> int | None:
    haystacks = [path.name, path.stem]
    for haystack in haystacks:
        for pattern in _STEP_PATTERNS:
            match = pattern.search(haystack)
            if match is not None:
                return int(match.group(1))
    return None
