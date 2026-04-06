from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ...execution.artifacts import normalize_workdirs, sync_patterns
from ..checkpoints import resolve_checkpoint_path, select_checkpoint_state, write_checkpoint_state


def collect_checkpoint_artifacts(
    *,
    result_dir: Path,
    workdirs: Iterable[Path | str] | None,
    checkpoint_globs: list[str] | tuple[str, ...],
    max_matches_per_glob: int,
) -> None:
    normalized_patterns = [str(pattern or "").strip() for pattern in checkpoint_globs if str(pattern or "").strip()]
    if not normalized_patterns or not workdirs:
        return
    sync_patterns(
        normalize_workdirs(workdirs),
        result_dir,
        normalized_patterns,
        "checkpoints",
        max(1, int(max_matches_per_glob)),
        warn_prefix="train_outputs",
    )


def resolve_latest_checkpoint(
    *,
    result_dir: Path,
    checkpoint_globs: list[str] | tuple[str, ...],
) -> tuple[Path | None, str, str]:
    latest_checkpoint: Path | None = None
    latest_selection_reason = ""
    selection_error = ""
    try:
        checkpoint_state = select_checkpoint_state(result_dir, checkpoint_globs)
        if checkpoint_state is not None:
            write_checkpoint_state(result_dir, checkpoint_state)
            latest_checkpoint = resolve_checkpoint_path(result_dir, checkpoint_state)
            latest_selection_reason = checkpoint_state.selection_reason
    except (FileNotFoundError, ValueError, TypeError) as exc:
        selection_error = str(exc)
    return latest_checkpoint, latest_selection_reason, selection_error


def resolve_explicit_checkpoint_path(
    *,
    explicit_checkpoint: str | None,
    result_dir: Path,
    checkpoint_dir: Path,
) -> tuple[Path | None, str]:
    if explicit_checkpoint is None:
        return None, ""

    raw_path = Path(explicit_checkpoint).expanduser()
    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path.resolve())
    else:
        candidates.append((checkpoint_dir / raw_path).resolve())
        candidates.append((result_dir / raw_path).resolve())

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate, "explicit_checkpoint"
    raise FileNotFoundError(
        f"explicit checkpoint `{explicit_checkpoint}` was not found under `{checkpoint_dir}` or `{result_dir}`"
    )
