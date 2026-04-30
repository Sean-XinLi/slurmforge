from __future__ import annotations

from dataclasses import dataclass

from ..slurm import SlurmClientProtocol, SlurmJobState
from .reconcile_rules import master_fallback


@dataclass(frozen=True)
class GroupSlurmObservation:
    job_id: str
    task_states: dict[int, SlurmJobState]
    fallback_state: SlurmJobState | None


def observe_group_slurm_state(
    *,
    client: SlurmClientProtocol,
    job_id: str,
    group_size: int,
) -> GroupSlurmObservation:
    job_states = client.query_observed_jobs([job_id])
    task_states: dict[int, SlurmJobState] = {}
    prefix = f"{job_id}_"
    for observed_job_id, observed_state in job_states.items():
        if observed_state.array_task_id is None:
            continue
        if observed_state.array_job_id == job_id or observed_job_id.startswith(prefix):
            task_states[observed_state.array_task_id] = observed_state
    return GroupSlurmObservation(
        job_id=job_id,
        task_states=task_states,
        fallback_state=master_fallback(job_states, job_id, group_size=group_size),
    )


def observed_task_state(
    observation: GroupSlurmObservation, task_index: int
) -> SlurmJobState | None:
    return observation.task_states.get(task_index) or observation.fallback_state
