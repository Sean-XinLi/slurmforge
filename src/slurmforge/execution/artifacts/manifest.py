from __future__ import annotations

from pathlib import Path


def build_artifact_manifest(
    *,
    workdirs: list[Path],
    result_dir: Path,
    copied: dict[str, list[str]],
    failures: dict[str, list[dict[str, str]]],
    max_matches_per_glob: int,
) -> dict[str, object]:
    failure_count = sum(len(items) for items in failures.values())
    return {
        "workdir": str(workdirs[0]),
        "workdirs": [str(path) for path in workdirs],
        "result_dir": str(result_dir),
        "copied": copied,
        "failures": failures,
        "max_matches_per_glob": max_matches_per_glob,
        "failure_count": failure_count,
        "status": "partial_failure" if failure_count else "ok",
    }
