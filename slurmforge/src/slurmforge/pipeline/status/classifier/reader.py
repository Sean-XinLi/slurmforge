from __future__ import annotations

from pathlib import Path

from ..models import AttemptResult
from .discovery import iter_log_paths


def read_text_excerpt(path: Path, limit: int = 200_000) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:limit]
    except Exception:
        return ""


def read_combined_log_text(
    attempt: AttemptResult | None,
    result_dir: Path,
    *,
    slurm_job_id: str = "",
) -> str:
    return "\n".join(
        read_text_excerpt(path)
        for path in iter_log_paths(
            attempt,
            result_dir,
            slurm_job_id=slurm_job_id,
        )
    )
