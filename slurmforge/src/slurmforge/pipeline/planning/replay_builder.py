from __future__ import annotations

from pathlib import Path

from ..config.api import ExperimentSpec, serialize_replay_experiment_spec
from ..records.replay_spec import build_replay_spec


def build_run_replay_spec(
    spec: ExperimentSpec,
    *,
    project_root: Path,
    replay_source_batch_root: str | None = None,
    replay_source_run_id: str | None = None,
    replay_source_record_path: str | None = None,
):
    return build_replay_spec(
        serialize_replay_experiment_spec(spec, project_root=project_root),
        planning_root=str(project_root),
        source_batch_root=replay_source_batch_root,
        source_run_id=replay_source_run_id,
        source_record_path=replay_source_record_path,
    )
