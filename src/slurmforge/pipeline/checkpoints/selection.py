from __future__ import annotations

from pathlib import Path

from ...errors import ConfigContractError
from .discovery import discover_checkpoint_files, extract_checkpoint_step
from .models import CheckpointState


def resolve_checkpoint_path(result_dir: Path, state: CheckpointState) -> Path:
    return (result_dir.resolve() / state.latest_checkpoint_rel).resolve()


def select_checkpoint_state(
    result_dir: Path,
    checkpoint_globs: list[str] | tuple[str, ...],
) -> CheckpointState | None:
    candidates = discover_checkpoint_files(result_dir, checkpoint_globs)
    if not candidates:
        return None

    if len(candidates) == 1:
        only = candidates[0].resolve()
        return CheckpointState(
            latest_checkpoint_rel=only.relative_to(result_dir.resolve()).as_posix(),
            selection_reason="single_candidate",
            global_step=extract_checkpoint_step(only),
        )

    ranked: list[tuple[int, str, Path]] = []
    for path in candidates:
        step = extract_checkpoint_step(path)
        if step is not None:
            ranked.append((step, path.relative_to(result_dir.resolve()).as_posix(), path))

    if not ranked:
        raise ConfigContractError(
            "Unable to deterministically select a checkpoint: multiple candidates exist and none expose a parseable step number"
        )

    step, rel_path, _selected = max(ranked, key=lambda item: (item[0], item[1]))
    return CheckpointState(
        latest_checkpoint_rel=rel_path,
        selection_reason="max_step",
        global_step=step,
    )
