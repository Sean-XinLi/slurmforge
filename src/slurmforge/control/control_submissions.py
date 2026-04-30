from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ..control_paths import control_submissions_path
from ..errors import ConfigContractError
from ..io import SchemaVersion, read_json, require_schema, utc_now, write_json

CONTROL_KIND_STAGE_INSTANCE_GATE = "stage_instance_gate"
CONTROL_KIND_DISPATCH_CATCHUP_GATE = "dispatch_catchup_gate"
CONTROL_KIND_TERMINAL_NOTIFICATION = "terminal_notification"

CONTROL_STATE_SUBMITTING = "submitting"
CONTROL_STATE_SUBMITTED = "submitted"
CONTROL_STATE_UNCERTAIN = "uncertain"
CONTROL_STATE_FAILED = "failed"


@dataclass(frozen=True)
class ControlSubmitResult:
    scheduler_job_ids: tuple[str, ...]
    barrier_job_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ControlSubmissionRecord:
    key: str
    kind: str
    target_kind: str
    target_id: str
    state: str
    sbatch_paths: tuple[str, ...] = ()
    scheduler_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()
    dependency_job_ids: tuple[str, ...] = ()
    reason: str = ""
    started_at: str = ""
    submitted_at: str = ""


def control_submission_key(kind: str, *, target_id: str) -> str:
    if not target_id:
        raise ConfigContractError(f"Control submission `{kind}` requires target_id")
    return f"{kind}:{target_id}"


def read_control_submissions(pipeline_root: Path) -> dict[str, Any]:
    path = control_submissions_path(pipeline_root)
    if path.exists():
        payload = read_json(path)
        require_schema(
            payload,
            name="control_submissions",
            version=SchemaVersion.CONTROL_SUBMISSIONS,
        )
        if isinstance(payload.get("submissions"), dict):
            return payload
    return {
        "schema_version": SchemaVersion.CONTROL_SUBMISSIONS,
        "updated_at": utc_now(),
        "submissions": {},
    }


def write_control_submissions(pipeline_root: Path, ledger: dict[str, Any]) -> None:
    ledger["schema_version"] = SchemaVersion.CONTROL_SUBMISSIONS
    ledger["updated_at"] = utc_now()
    submissions = ledger.setdefault("submissions", {})
    if not isinstance(submissions, dict):
        ledger["submissions"] = {}
    write_json(control_submissions_path(pipeline_root), ledger)


def submitted_control_records(pipeline_root: Path) -> dict[str, dict[str, Any]]:
    ledger = read_control_submissions(pipeline_root)
    return {
        key: dict(record)
        for key, record in dict(ledger.get("submissions") or {}).items()
        if isinstance(record, dict) and record.get("state") == CONTROL_STATE_SUBMITTED
    }


def submitted_control_job_ids(pipeline_root: Path) -> dict[str, str]:
    records = submitted_control_records(pipeline_root)
    result: dict[str, str] = {}
    for key, record in records.items():
        job_ids = tuple(str(item) for item in record.get("scheduler_job_ids") or ())
        if job_ids:
            result[key] = job_ids[0]
    return result


def control_submission_from_payload(
    key: str, payload: dict[str, Any]
) -> ControlSubmissionRecord:
    return ControlSubmissionRecord(
        key=key,
        kind=str(payload["kind"]),
        target_kind=str(payload.get("target_kind") or ""),
        target_id=str(payload.get("target_id") or ""),
        state=str(payload["state"]),
        sbatch_paths=tuple(str(item) for item in payload.get("sbatch_paths") or ()),
        scheduler_job_ids=tuple(
            str(item) for item in payload.get("scheduler_job_ids") or ()
        ),
        barrier_job_ids=tuple(str(item) for item in payload.get("barrier_job_ids") or ()),
        dependency_job_ids=tuple(
            str(item) for item in payload.get("dependency_job_ids") or ()
        ),
        reason=str(payload.get("reason") or ""),
        started_at=str(payload.get("started_at") or ""),
        submitted_at=str(payload.get("submitted_at") or ""),
    )


def submit_control_once(
    pipeline_root: Path,
    *,
    key: str,
    kind: str,
    target_kind: str,
    target_id: str,
    sbatch_paths: tuple[Path, ...],
    dependency_job_ids: tuple[str, ...],
    submitter: Callable[[], ControlSubmitResult],
) -> ControlSubmissionRecord:
    ledger = read_control_submissions(pipeline_root)
    submissions = ledger.setdefault("submissions", {})
    existing = submissions.get(key)
    if isinstance(existing, dict):
        if existing.get("state") == CONTROL_STATE_SUBMITTED and existing.get(
            "scheduler_job_ids"
        ):
            return control_submission_from_payload(key, existing)
        if existing.get("state") == CONTROL_STATE_SUBMITTING:
            existing["state"] = CONTROL_STATE_UNCERTAIN
            existing["reason"] = (
                "previous submission reached scheduler call without recorded job ids"
            )
            write_control_submissions(pipeline_root, ledger)
            raise ConfigContractError(
                f"Control submission is uncertain for `{key}`; inspect "
                f"{control_submissions_path(pipeline_root)} before retrying"
            )

    submissions[key] = {
        "state": CONTROL_STATE_SUBMITTING,
        "kind": kind,
        "target_kind": target_kind,
        "target_id": target_id,
        "sbatch_paths": [str(path) for path in sbatch_paths],
        "dependency_job_ids": list(dependency_job_ids),
        "started_at": utc_now(),
    }
    write_control_submissions(pipeline_root, ledger)

    try:
        result = submitter()
    except Exception as exc:
        submissions[key].update(
            {
                "state": CONTROL_STATE_UNCERTAIN,
                "reason": str(exc),
                "failed_at": utc_now(),
            }
        )
        write_control_submissions(pipeline_root, ledger)
        raise

    submissions[key].update(
        {
            "state": CONTROL_STATE_SUBMITTED,
            "scheduler_job_ids": list(result.scheduler_job_ids),
            "barrier_job_ids": list(result.barrier_job_ids),
            "submitted_at": utc_now(),
        }
    )
    write_control_submissions(pipeline_root, ledger)
    return control_submission_from_payload(key, submissions[key])
