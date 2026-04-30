from __future__ import annotations

import subprocess
from pathlib import Path

from .models import SlurmJobState, SlurmSubmitOptions
from .parsers import parse_sacct_rows, parse_sbatch_job_id, parse_squeue_rows


class SlurmClient:
    def submit(
        self, path: Path, *, options: SlurmSubmitOptions | None = None
    ) -> str:
        submit_options = options or SlurmSubmitOptions()
        cmd = ["sbatch", "--parsable"]
        if submit_options.dependency:
            cmd.append(f"--dependency={submit_options.dependency}")
        if submit_options.mail_user:
            cmd.append(f"--mail-user={submit_options.mail_user}")
        if submit_options.mail_type:
            cmd.append(f"--mail-type={submit_options.mail_type}")
        cmd.append(str(path))
        proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return parse_sbatch_job_id(proc.stdout)

    def query_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]:
        if not job_ids:
            return {}
        joined = ",".join(job_ids)
        cmd = [
            "sacct",
            "-P",
            "-n",
            "-j",
            joined,
            "--format=JobID,JobIDRaw,ArrayJobID,ArrayTaskID,State,ExitCode,Reason",
        ]
        proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return parse_sacct_rows(proc.stdout)

    def query_active_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]:
        if not job_ids:
            return {}
        joined = ",".join(job_ids)
        cmd = ["squeue", "-h", "-j", joined, "-o", "%i|%T|%R"]
        proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return parse_squeue_rows(proc.stdout)

    def query_observed_jobs(self, job_ids: list[str]) -> dict[str, SlurmJobState]:
        observed: dict[str, SlurmJobState] = {}
        sacct_error: Exception | None = None
        squeue_error: Exception | None = None
        try:
            observed.update(self.query_jobs(job_ids))
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            sacct_error = exc
        try:
            observed.update(self.query_active_jobs(job_ids))
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            squeue_error = exc
        if not observed and sacct_error is not None and squeue_error is not None:
            raise sacct_error
        return observed

    def query_array_tasks(self, array_job_id: str) -> dict[int, SlurmJobState]:
        states = self.query_observed_jobs([array_job_id])
        tasks: dict[int, SlurmJobState] = {}
        prefix = f"{array_job_id}_"
        for job_id, state in states.items():
            if state.array_task_id is None:
                continue
            if state.array_job_id == array_job_id or job_id.startswith(prefix):
                tasks[state.array_task_id] = state
        return tasks
