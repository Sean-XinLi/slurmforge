from __future__ import annotations

import re

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
        exit_code = (
            parts[state_index + 1].strip() if len(parts) > state_index + 1 else ""
        )
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
