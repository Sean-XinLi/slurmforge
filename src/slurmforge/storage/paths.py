"""Filesystem path conventions for stage run directories."""

from __future__ import annotations

from pathlib import Path


def status_path(run_dir: Path) -> Path:
    return run_dir / "status.json"


def stage_plan_path(run_dir: Path) -> Path:
    return run_dir / "stage_plan.json"


def input_bindings_path(run_dir: Path) -> Path:
    return run_dir / "input_bindings.json"


def stage_outputs_path(run_dir: Path) -> Path:
    return run_dir / "stage_outputs.json"


def status_events_path(run_dir: Path) -> Path:
    return run_dir / "status_events.jsonl"


def attempts_dir(run_dir: Path) -> Path:
    return run_dir / "attempts"


def attempt_path(run_dir: Path, attempt_id: str) -> Path:
    return attempts_dir(run_dir) / attempt_id / "attempt.json"


def root_ref_path(run_dir: Path) -> Path:
    return Path(run_dir) / "root_ref.json"


def stage_catalog_path(pipeline_root: Path) -> Path:
    return Path(pipeline_root) / "execution" / "stage_catalog.json"


def runtime_batches_path(pipeline_root: Path) -> Path:
    return Path(pipeline_root) / "execution" / "runtime_batches.json"


def next_attempt_id(run_dir: Path) -> str:
    root = attempts_dir(run_dir)
    if not root.exists():
        return "0001"
    existing = [
        int(path.name)
        for path in root.iterdir()
        if path.is_dir() and path.name.isdigit()
    ]
    return f"{(max(existing) + 1) if existing else 1:04d}"
