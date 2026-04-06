from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ..models import AttemptResult


def iter_array_log_dirs(run_dir: Path) -> Iterable[Path]:
    seen: set[Path] = set()
    for ancestor in (run_dir, *run_dir.parents):
        candidate = ancestor / "array_logs"
        if not candidate.is_dir():
            continue
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        yield candidate


def iter_log_paths(
    attempt: AttemptResult | None,
    result_dir: Path,
    *,
    slurm_job_id: str = "",
) -> Iterable[Path]:
    seen: set[Path] = set()

    def _yield(path: Path) -> Iterable[Path]:
        resolved = path.resolve()
        if resolved in seen:
            return ()
        seen.add(resolved)
        return (path,)

    if attempt is not None:
        for raw in (attempt.train_log, attempt.eval_log, attempt.slurm_out, attempt.slurm_err):
            if raw:
                for path in _yield(Path(raw)):
                    yield path
    log_dir = result_dir / "logs"
    if log_dir.exists():
        for candidate in sorted(log_dir.iterdir()):
            if candidate.is_file():
                for path in _yield(candidate):
                    yield path
    run_dir = result_dir.parent
    if slurm_job_id:
        for candidate in (run_dir / f"slurm-{slurm_job_id}.out", run_dir / f"slurm-{slurm_job_id}.err"):
            if candidate.exists():
                for path in _yield(candidate):
                    yield path
        for array_log_dir in iter_array_log_dirs(run_dir):
            for candidate in (
                array_log_dir / f"slurm-{slurm_job_id}.out",
                array_log_dir / f"slurm-{slurm_job_id}.err",
            ):
                if candidate.exists():
                    for path in _yield(candidate):
                        yield path
