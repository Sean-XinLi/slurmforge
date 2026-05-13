from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..io import (
    SchemaVersion,
    read_json_object,
    to_jsonable,
    utc_now,
    write_json_object,
)
from .models import GroupSubmissionRecord, SubmissionLedger, SubmitGeneration
from .ledger_records import (
    GROUP_JOB_STATES,
    submission_ledger_from_dict,
    validate_ledger_matches_generation,
    validate_submission_ledger,
)


def submissions_dir(batch_root: Path) -> Path:
    return batch_root / "submissions"


def ledger_path(batch_root: Path) -> Path:
    return submissions_dir(batch_root) / "ledger.json"


def submission_events_path(batch_root: Path) -> Path:
    return submissions_dir(batch_root) / "events.jsonl"


def append_submission_event(batch_root: Path, event: str, **payload: Any) -> None:
    path = submission_events_path(batch_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "at": utc_now(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(record), sort_keys=True) + "\n")


def read_submission_ledger(batch_root: Path) -> SubmissionLedger | None:
    path = ledger_path(batch_root)
    if not path.exists():
        return None
    return submission_ledger_from_dict(read_json_object(path))


def write_submission_ledger(batch_root: Path, ledger: SubmissionLedger) -> None:
    validate_submission_ledger(ledger)
    write_json_object(ledger_path(batch_root), ledger)


def initialize_submission_ledger(
    batch_root: Path, generation: SubmitGeneration
) -> SubmissionLedger:
    existing = read_submission_ledger(batch_root)
    if existing is not None:
        if (
            existing.batch_id != generation.batch_id
            or existing.stage_name != generation.stage_name
        ):
            raise ConfigContractError(
                f"Submission ledger under {batch_root} belongs to "
                f"{existing.stage_name}/{existing.batch_id}, not {generation.stage_name}/{generation.batch_id}"
            )
        submitted = [
            group
            for group in existing.groups.values()
            if group.scheduler_job_id and group.state in {"submitted", "adopted"}
        ]
        if existing.generation_id != generation.generation_id and submitted:
            raise ConfigContractError(
                f"Submission ledger under {batch_root} already has submitted jobs for generation "
                f"{existing.generation_id}; refusing to switch to {generation.generation_id}"
            )
        if existing.generation_id == generation.generation_id:
            for group_id, sbatch_path in generation.sbatch_paths_by_group.items():
                existing.groups.setdefault(
                    group_id,
                    GroupSubmissionRecord(group_id=group_id, sbatch_path=sbatch_path),
                )
            validate_ledger_matches_generation(existing, generation)
            write_submission_ledger(batch_root, existing)
            return existing

    ledger = SubmissionLedger(
        schema_version=SchemaVersion.SUBMISSION_LEDGER,
        batch_id=generation.batch_id,
        stage_name=generation.stage_name,
        generation_id=generation.generation_id,
        state="planned",
        groups={
            group_id: GroupSubmissionRecord(group_id=group_id, sbatch_path=sbatch_path)
            for group_id, sbatch_path in generation.sbatch_paths_by_group.items()
        },
    )
    write_submission_ledger(batch_root, ledger)
    append_submission_event(
        batch_root, "ledger_initialized", generation_id=generation.generation_id
    )
    return ledger


def submitted_group_job_ids(batch_root: Path) -> dict[str, str]:
    ledger = read_submission_ledger(batch_root)
    if ledger is None:
        return {}
    return {
        group_id: str(group.scheduler_job_id)
        for group_id, group in ledger.groups.items()
        if group.scheduler_job_id and group.state in GROUP_JOB_STATES
    }


def ledger_state(batch_root: Path) -> str:
    ledger = read_submission_ledger(batch_root)
    return "missing" if ledger is None else ledger.state
