from __future__ import annotations

from typing import Any

def build_run_snapshot(
    *,
    run_index: int,
    total_runs: int,
    run_id: str,
    generated_by: Any,
    project: str,
    experiment_name: str,
    model_name: str,
    train_mode: str,
    replay_spec: Any,
    sweep_case_name: str | None = None,
    sweep_assignments: dict[str, Any] | None = None,
) -> Any:
    from ..records.models.run_snapshot import RunSnapshot

    return RunSnapshot(
        run_index=run_index,
        total_runs=total_runs,
        run_id=run_id,
        generated_by=generated_by,
        project=project,
        experiment_name=experiment_name,
        model_name=model_name,
        train_mode=train_mode,
        sweep_case_name=sweep_case_name,
        sweep_assignments={} if sweep_assignments is None else dict(sweep_assignments),
        replay_spec=replay_spec,
    )
