from __future__ import annotations

from pathlib import Path

from ...errors import ConfigContractError

PRIMARY_POLICIES = {"latest", "best", "explicit"}


def normalize_primary_policy(primary_policy: str | None) -> str:
    normalized_policy = str(primary_policy or "latest").strip().lower()
    if normalized_policy not in PRIMARY_POLICIES:
        raise ConfigContractError(f"primary_policy must be one of: {sorted(PRIMARY_POLICIES)}")
    return normalized_policy


def normalize_explicit_checkpoint(explicit_checkpoint: str | None) -> str | None:
    text = str(explicit_checkpoint or "").strip()
    return text or None


def checkpoint_sort_key(path: Path, *, root: Path, extract_checkpoint_step) -> tuple[int, str]:
    step = extract_checkpoint_step(path)
    rel = path.resolve().relative_to(root.resolve()).as_posix()
    return (-1 if step is None else int(step), rel)


def best_checkpoint_path(
    candidates: list[Path],
    *,
    root: Path,
    extract_checkpoint_step,
) -> Path | None:
    best_named = [path for path in candidates if "best" in path.name.lower() or "best" in path.stem.lower()]
    if not best_named:
        return None
    return max(best_named, key=lambda item: checkpoint_sort_key(item, root=root, extract_checkpoint_step=extract_checkpoint_step))


def resolve_primary_checkpoint(
    *,
    primary_policy: str,
    latest_checkpoint: Path | None,
    best_checkpoint: Path | None,
    explicit_checkpoint: Path | None,
    latest_selection_reason: str,
) -> tuple[Path | None, str]:
    if primary_policy == "explicit":
        if explicit_checkpoint is not None:
            return explicit_checkpoint, "explicit_checkpoint"
        return None, ""
    if primary_policy == "best":
        if best_checkpoint is not None:
            return best_checkpoint, "best_named_candidate"
        if latest_checkpoint is not None:
            return latest_checkpoint, (
                f"best_missing_fallback:{latest_selection_reason}" if latest_selection_reason else "best_missing_fallback:latest"
            )
        return None, ""
    if latest_checkpoint is not None:
        return latest_checkpoint, latest_selection_reason or "latest_checkpoint"
    if best_checkpoint is not None:
        return best_checkpoint, "latest_missing_fallback:best_named_candidate"
    return None, ""
