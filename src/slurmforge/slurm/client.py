from __future__ import annotations

import re
import subprocess
from pathlib import Path

from .models import SlurmJobState, normalize_slurm_state


def parse_sbatch_job_id(output: str) -> str:
    cleaned = output.strip()
    if not cleaned:
        raise RuntimeError("sbatch returned an empty job id")
    match = re.search(r"Submitted batch job\s+(\S+)", cleaned)
    if match:
        return match.group(1)
    return cleaned.splitlines()[-1].strip()


def parse_sacct_rows(output: str) -> dict[str, SlurmJobState]:
    states: dict[str, SlurmJobState] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 7:
            job_id = parts[0].strip()
            job_id_raw = parts[1].strip()
            array_job_id = _clean_array_field(parts[2])
            array_task_id = _parse_array_task_id(parts[3])
            state_index = 4
        elif len(parts) >= 2:
            job_id = parts[0].strip()
            job_id_raw = job_id
            array_job_id = None
            array_task_id = _task_id_from_job_id(job_id)
            state_index = 1
        else:
            continue
        if _is_job_step(job_id):
            continue
        state = normalize_slurm_state(parts[state_index])
        exit_code = parts[state_index + 1].strip() if len(parts) > state_index + 1 else ""
        reason = parts[state_index + 2].strip() if len(parts) > state_index + 2 else ""
        if array_job_id is None and "_" in job_id:
            array_job_id = job_id.split("_", 1)[0]
        if array_task_id is None:
            array_task_id = _task_id_from_job_id(job_id)
        states[job_id] = SlurmJobState(
            job_id=job_id,
            state=state,
            exit_code=exit_code,
            reason=reason,
            array_job_id=array_job_id,
            array_task_id=array_task_id,
            job_id_raw=job_id_raw,
        )
    return states


def parse_squeue_rows(output: str) -> dict[str, SlurmJobState]:
    states: dict[str, SlurmJobState] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        job_id = parts[0].strip()
        if not job_id or _is_job_step(job_id):
            continue
        reason = parts[2].strip() if len(parts) > 2 else ""
        array_job_id = job_id.split("_", 1)[0] if "_" in job_id else None
        array_task_id = _task_id_from_job_id(job_id)
        states[job_id] = SlurmJobState(
            job_id=job_id,
            state=normalize_slurm_state(parts[1]),
            reason=reason,
            array_job_id=array_job_id,
            array_task_id=array_task_id,
            job_id_raw=job_id,
        )
    return states


def _clean_array_field(value: str) -> str | None:
    cleaned = value.strip()
    if cleaned in {"", "Unknown", "None", "N/A", "4294967294"}:
        return None
    return cleaned


def _parse_array_task_id(value: str) -> int | None:
    cleaned = value.strip()
    if cleaned in {"", "Unknown", "None", "N/A", "4294967294"}:
        return None
    if cleaned.isdigit():
        return int(cleaned)
    return None


def _task_id_from_job_id(job_id: str) -> int | None:
    if "_" not in job_id:
        return None
    raw_task = job_id.rsplit("_", 1)[1]
    if raw_task.isdigit():
        return int(raw_task)
    return None


def _is_job_step(job_id: str) -> bool:
    return "." in job_id


class SlurmClient:
    def submit(self, path: Path, *, dependency: str | None = None) -> str:
        cmd = ["sbatch", "--parsable"]
        if dependency:
            cmd.append(f"--dependency={dependency}")
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


def _fake_array_job_id(job_id: str) -> str | None:
    return job_id.split("_", 1)[0] if "_" in job_id else None


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

    def set_job_state(self, job_id: str, state: str, *, exit_code: str = "", reason: str = "") -> None:
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
