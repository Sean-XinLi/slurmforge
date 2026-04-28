from __future__ import annotations

from pathlib import Path

from slurmforge.slurm import SlurmClient, SlurmJobState, normalize_slurm_state


class FakeSlurmClient(SlurmClient):
    def __init__(self) -> None:
        self._next_job_id = 1000
        self.submissions: list[tuple[Path, str | None, str]] = []
        self.job_states: dict[str, SlurmJobState] = {}

    def submit(self, path: Path, *, dependency: str | None = None) -> str:
        self._next_job_id += 1
        job_id = str(self._next_job_id)
        self.submissions.append((path, dependency, job_id))
        self.job_states[job_id] = SlurmJobState(job_id=job_id, state="PENDING")
        return job_id

    def set_job_state(
        self, job_id: str, state: str, *, exit_code: str = "", reason: str = ""
    ) -> None:
        self.job_states[job_id] = SlurmJobState(
            job_id=job_id,
            state=normalize_slurm_state(state),
            exit_code=exit_code,
            reason=reason,
            array_job_id=_fake_array_job_id(job_id),
            array_task_id=_task_id_from_job_id(job_id),
            job_id_raw=job_id,
        )

    def set_array_task_state(
        self,
        array_job_id: str,
        task_id: int,
        state: str,
        *,
        exit_code: str = "",
        reason: str = "",
    ) -> None:
        job_id = f"{array_job_id}_{task_id}"
        self.job_states[job_id] = SlurmJobState(
            job_id=job_id,
            state=normalize_slurm_state(state),
            exit_code=exit_code,
            reason=reason,
            array_job_id=array_job_id,
            array_task_id=task_id,
            job_id_raw=job_id,
        )

    def query_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]:
        if not job_ids:
            return {}
        result: dict[str, SlurmJobState] = {}
        for requested in job_ids:
            if requested in self.job_states:
                result[requested] = self.job_states[requested]
            prefix = f"{requested}_"
            for job_id, state in self.job_states.items():
                if job_id.startswith(prefix):
                    result[job_id] = state
        return result

    def query_active_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]:
        return self.query_jobs(job_ids)

    def query_observed_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]:
        return self.query_jobs(job_ids)


def _fake_array_job_id(job_id: str) -> str | None:
    return job_id.split("_", 1)[0] if "_" in job_id else None


def _task_id_from_job_id(job_id: str) -> int | None:
    if "_" not in job_id:
        return None
    raw_task = job_id.rsplit("_", 1)[1]
    if raw_task.isdigit():
        return int(raw_task)
    return None
