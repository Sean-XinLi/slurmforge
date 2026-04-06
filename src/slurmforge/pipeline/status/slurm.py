from __future__ import annotations

from dataclasses import dataclass
import subprocess


@dataclass(frozen=True)
class SlurmJobState:
    state: str
    raw_state: str
    failure_class: str | None = None
    reason: str = ""


def query_slurm_job_state(slurm_job_id: str) -> SlurmJobState | None:
    job_id = str(slurm_job_id or "").strip()
    if not job_id:
        return None

    active = _query_squeue(job_id)
    if active is not None:
        return active
    return _query_sacct(job_id)


def _query_squeue(job_id: str) -> SlurmJobState | None:
    try:
        completed = subprocess.run(
            ["squeue", "-h", "-j", job_id, "-o", "%T"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, PermissionError, subprocess.SubprocessError, OSError):
        return None
    if completed.returncode != 0:
        return None

    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        return None
    return _normalize_slurm_state(lines[0], source="squeue")


def _query_sacct(job_id: str) -> SlurmJobState | None:
    try:
        completed = subprocess.run(
            ["sacct", "-n", "-P", "-j", job_id, "--format=State"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, PermissionError, subprocess.SubprocessError, OSError):
        return None
    if completed.returncode != 0:
        return None

    raw_states = [line.strip().split("|", 1)[0] for line in completed.stdout.splitlines() if line.strip()]
    if not raw_states:
        return None
    for raw_state in raw_states:
        normalized = _normalize_slurm_state(raw_state, source="sacct")
        if normalized is not None:
            return normalized
    return None


def _normalize_slurm_state(raw_state: str, *, source: str) -> SlurmJobState | None:
    text = str(raw_state or "").strip()
    if not text:
        return None
    canonical = text.split()[0].split("+", 1)[0].strip().upper()
    if not canonical:
        return None

    if canonical in {"PENDING", "CONFIGURING"}:
        return SlurmJobState(state="pending", raw_state=text, reason=f"reported by {source}: {text}")
    if canonical in {"RUNNING", "COMPLETING", "STAGE_OUT", "RESIZING", "SUSPENDED"}:
        return SlurmJobState(state="running", raw_state=text, reason=f"reported by {source}: {text}")
    if canonical in {"COMPLETED"}:
        return SlurmJobState(state="success", raw_state=text, reason=f"reported by {source}: {text}")
    if canonical in {"OUT_OF_MEMORY"}:
        return SlurmJobState(state="failed", raw_state=text, failure_class="oom", reason=f"reported by {source}: {text}")
    if canonical in {"PREEMPTED", "REQUEUED", "REQUEUE_HOLD", "REQUEUE_FED"}:
        return SlurmJobState(
            state="failed",
            raw_state=text,
            failure_class="preempted",
            reason=f"reported by {source}: {text}",
        )
    if canonical in {"NODE_FAIL", "BOOT_FAIL"}:
        return SlurmJobState(
            state="failed",
            raw_state=text,
            failure_class="node_failure",
            reason=f"reported by {source}: {text}",
        )
    if canonical in {"FAILED", "CANCELLED", "TIMEOUT", "DEADLINE", "OOM", "STOPPED"}:
        return SlurmJobState(
            state="failed",
            raw_state=text,
            failure_class="executor_error",
            reason=f"reported by {source}: {text}",
        )
    return SlurmJobState(state="running", raw_state=text, reason=f"reported by {source}: {text}")
