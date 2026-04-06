from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...config.api import ExperimentSpec
from ..fingerprint import run_id, user_run_id_payload


@dataclass(frozen=True)
class ResolvedRunIdentity:
    run_id: str
    run_dir: Path
    model_name: str


def resolve_run_identity(
    spec: ExperimentSpec,
    *,
    train_mode: str,
    model_name: str,
    project_root: Path,
    batch_root: Path,
    run_index: int,
) -> ResolvedRunIdentity:
    resolved_run_id = run_id(
        user_run_id_payload(
            spec,
            train_mode=train_mode,
            model_name=model_name,
            project_root=project_root,
        )
    )
    return ResolvedRunIdentity(
        run_id=resolved_run_id,
        run_dir=batch_root / "runs" / f"run_{run_index:03d}_{resolved_run_id}",
        model_name=model_name,
    )
