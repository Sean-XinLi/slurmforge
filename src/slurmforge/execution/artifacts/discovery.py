from __future__ import annotations

from pathlib import Path
from typing import Iterable


def is_relative_to(path: Path, root: Path) -> bool:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    try:
        resolved_path.relative_to(resolved_root)
        return True
    except ValueError:
        return False


def collect_matches(
    workdir: Path,
    pattern: str,
    exclude_root: Path | None = None,
    *,
    max_matches: int,
    warn_prefix: str = "artifact_sync",
) -> list[Path]:
    if Path(pattern).is_absolute():
        iterator = Path("/").glob(pattern.lstrip("/"))
    else:
        iterator = workdir.glob(pattern)

    out: list[Path] = []
    for candidate in iterator:
        if not candidate.exists():
            continue
        if exclude_root is not None and is_relative_to(candidate, exclude_root):
            continue
        out.append(candidate)
        if len(out) >= max_matches:
            print(
                f"[{warn_prefix}][WARN] pattern `{pattern}` reached max_matches_per_glob={max_matches}, truncating."
            )
            break
    return out


def normalize_workdirs(value: Path | str | Iterable[Path | str]) -> list[Path]:
    raw_items: Iterable[Path | str]
    if isinstance(value, (str, Path)):
        raw_items = [value]
    else:
        raw_items = value
    out: list[Path] = []
    seen: set[Path] = set()
    for item in raw_items:
        path = Path(item).expanduser().resolve()
        if path in seen:
            continue
        seen.add(path)
        out.append(path)
    if not out:
        raise ValueError("at least one workdir must be provided")
    return out
