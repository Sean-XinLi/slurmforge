from __future__ import annotations

from pathlib import Path
from typing import Protocol

from .models import SlurmJobState, SlurmSubmitOptions


class SlurmClientProtocol(Protocol):
    def submit(
        self, path: Path, *, options: SlurmSubmitOptions | None = None
    ) -> str: ...

    def query_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]: ...

    def query_active_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]: ...

    def query_observed_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]: ...

    def query_array_tasks(self, array_job_id: str) -> dict[int, SlurmJobState]: ...
