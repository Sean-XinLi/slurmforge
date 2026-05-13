from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..storage.paths import stage_outputs_path
from ..slurm import (
    SlurmJobState,
    failure_class_for_slurm_state,
    stage_state_for_slurm_state,
)
from .reconcile_observations import missing_output_expired


@dataclass(frozen=True)
class StageReconcileDecision:
    stage_state: str
    failure_class: str | None
    reason: str


def scheduler_stage_state(slurm_state: SlurmJobState) -> str | None:
    return stage_state_for_slurm_state(slurm_state.state)


def decide_stage_status(
    *,
    run_dir: Path,
    slurm_state: SlurmJobState,
    initial_stage_state: str,
    missing_output_grace_seconds: int,
) -> StageReconcileDecision:
    if initial_stage_state == "success" and not stage_outputs_path(run_dir).exists():
        if missing_output_expired(
            run_dir,
            slurm_state=slurm_state.state,
            grace_seconds=missing_output_grace_seconds,
        ):
            return StageReconcileDecision(
                stage_state="failed",
                failure_class="missing_attempt_result",
                reason=(
                    f"Slurm job {slurm_state.job_id} completed but no "
                    "stage_outputs.json was written"
                ),
            )
        return StageReconcileDecision(
            stage_state="running",
            failure_class=None,
            reason=(
                f"Slurm job {slurm_state.job_id} completed; waiting for "
                f"stage_outputs.json for up to {missing_output_grace_seconds}s"
            ),
        )
    reason = f"Slurm job {slurm_state.job_id} state={slurm_state.state}"
    if slurm_state.reason:
        reason = f"{reason} reason={slurm_state.reason}"
    return StageReconcileDecision(
        stage_state=initial_stage_state,
        failure_class=failure_class_for_slurm_state(slurm_state.state),
        reason=reason,
    )
