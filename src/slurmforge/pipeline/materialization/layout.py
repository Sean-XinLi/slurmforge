from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from .context import MaterializationLayout


def map_to_staging(final_path: Path, *, final_root: Path, staging_root: Path) -> Path:
    resolved_root = final_root.resolve()
    resolved_path = final_path.resolve()
    return staging_root / resolved_path.relative_to(resolved_root)


def resolve_materialization_layout(batch_root: Path) -> MaterializationLayout:
    final_batch_root = batch_root.resolve()
    final_sbatch_dir = (final_batch_root / "sbatch").resolve()
    if final_batch_root.exists():
        raise FileExistsError(f"batch_root already exists: {final_batch_root}")

    staging_root = final_batch_root.parent / f".{final_batch_root.name}.staging-{uuid4().hex[:10]}"
    if staging_root.exists():
        raise FileExistsError(f"staging_root already exists: {staging_root}")

    return MaterializationLayout(
        final_batch_root=final_batch_root,
        final_sbatch_dir=final_sbatch_dir,
        final_notify_sbatch=final_sbatch_dir / "notify.sbatch.sh",
        staging_root=staging_root,
        submit_script=final_sbatch_dir / "submit_all.sh",
        manifest_path=final_batch_root / "batch_manifest.json",
        runs_manifest_path=final_batch_root / "meta" / "runs_manifest.jsonl",
        array_log_dir=final_batch_root / "array_logs",
    )


def prepare_staging_layout(layout: MaterializationLayout) -> None:
    layout.staging_root.mkdir(parents=True, exist_ok=False)
    map_to_staging(layout.final_sbatch_dir, final_root=layout.final_batch_root, staging_root=layout.staging_root).mkdir(
        parents=True, exist_ok=True
    )
    map_to_staging(
        layout.runs_manifest_path.parent,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    ).mkdir(parents=True, exist_ok=True)
    map_to_staging(layout.array_log_dir, final_root=layout.final_batch_root, staging_root=layout.staging_root).mkdir(
        parents=True, exist_ok=True
    )
