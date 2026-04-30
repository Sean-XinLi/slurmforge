from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ..emit.pipeline_gate import (
    write_pipeline_gate_barrier_file,
    write_pipeline_gate_submit_file,
)
from ..workflow_contract import (
    EVAL_SHARD_GATE,
    FINAL_GATE,
    TRAIN_GROUP_GATE,
)
from ..errors import ConfigContractError
from ..control_paths import gate_ledger_path
from ..io import SchemaVersion, read_json, utc_now, write_json
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import submit_dependent_job_with_dependency_tree


@dataclass(frozen=True)
class GateSubmissionRecord:
    key: str
    gate: str
    group_id: str
    scheduler_job_id: str
    sbatch_path: str
    barrier_job_ids: tuple[str, ...]
    dependency_job_ids: tuple[str, ...]
    state: str


def gate_ledger_key(gate: str, *, group_id: str | None = None) -> str:
    if gate == TRAIN_GROUP_GATE:
        return f"train_group:{group_id}"
    if gate == EVAL_SHARD_GATE:
        return f"eval_shard:{group_id}"
    if gate == FINAL_GATE:
        return FINAL_GATE
    raise ConfigContractError(f"Unsupported pipeline gate: {gate}")


def read_gate_ledger(pipeline_root: Path) -> dict[str, Any]:
    path = gate_ledger_path(pipeline_root)
    if path.exists():
        payload = read_json(path)
        if isinstance(payload.get("gates"), dict):
            return payload
    return {
        "schema_version": SchemaVersion.GATE_LEDGER,
        "updated_at": utc_now(),
        "gates": {},
    }


def write_gate_ledger(pipeline_root: Path, ledger: dict[str, Any]) -> None:
    ledger["schema_version"] = SchemaVersion.GATE_LEDGER
    ledger["updated_at"] = utc_now()
    gates = ledger.setdefault("gates", {})
    if not isinstance(gates, dict):
        ledger["gates"] = {}
    write_json(gate_ledger_path(pipeline_root), ledger)


def submitted_gate_records(pipeline_root: Path) -> dict[str, dict[str, Any]]:
    ledger = read_gate_ledger(pipeline_root)
    return {
        key: dict(record)
        for key, record in dict(ledger.get("gates") or {}).items()
        if isinstance(record, dict) and record.get("state") == "submitted"
    }


def _record_from_payload(key: str, payload: dict[str, Any]) -> GateSubmissionRecord:
    return GateSubmissionRecord(
        key=key,
        gate=str(payload["gate"]),
        group_id=str(payload.get("group_id") or ""),
        scheduler_job_id=str(payload["scheduler_job_id"]),
        sbatch_path=str(payload["sbatch_path"]),
        barrier_job_ids=tuple(str(item) for item in payload.get("barrier_job_ids") or ()),
        dependency_job_ids=tuple(
            str(item) for item in payload.get("dependency_job_ids") or ()
        ),
        state=str(payload["state"]),
    )


def submit_gate_once(
    pipeline_root: Path,
    plan,
    gate: str,
    *,
    dependency_job_ids: tuple[str, ...],
    client: SlurmClientProtocol,
    group_id: str | None = None,
    max_dependency_length: int,
    barrier_path_factory: Callable[[int], Path] | None = None,
) -> GateSubmissionRecord:
    key = gate_ledger_key(gate, group_id=group_id)
    ledger = read_gate_ledger(pipeline_root)
    gates = ledger.setdefault("gates", {})
    existing = gates.get(key)
    if isinstance(existing, dict):
        if existing.get("state") == "submitted" and existing.get("scheduler_job_id"):
            return _record_from_payload(key, existing)
        if existing.get("state") == "submitting" and not existing.get(
            "scheduler_job_id"
        ):
            existing["state"] = "uncertain"
            existing["reason"] = (
                "previous submission reached scheduler call without a recorded job id"
            )
            write_gate_ledger(pipeline_root, ledger)
            raise ConfigContractError(
                f"Control gate submission is uncertain for `{key}`; inspect "
                f"{gate_ledger_path(pipeline_root)} before retrying"
            )

    gate_path = write_pipeline_gate_submit_file(plan, gate, group_id=group_id)
    gates[key] = {
        "state": "submitting",
        "gate": gate,
        "group_id": group_id or "",
        "sbatch_path": str(gate_path),
        "dependency_job_ids": list(dependency_job_ids),
        "started_at": utc_now(),
    }
    write_gate_ledger(pipeline_root, ledger)

    try:
        gate_job_id, barrier_job_ids = submit_dependent_job_with_dependency_tree(
            target_path=gate_path,
            dependency_job_ids=dependency_job_ids,
            client=client,
            max_dependency_length=max_dependency_length,
            barrier_path_factory=barrier_path_factory
            or (
                lambda barrier_index: write_pipeline_gate_barrier_file(
                    plan,
                    gate,
                    group_id=group_id,
                    barrier_index=barrier_index,
                )
            ),
        )
    except Exception as exc:
        gates[key].update(
            {
                "state": "uncertain",
                "reason": str(exc),
                "failed_at": utc_now(),
            }
        )
        write_gate_ledger(pipeline_root, ledger)
        raise

    gates[key].update(
        {
            "state": "submitted",
            "scheduler_job_id": gate_job_id,
            "barrier_job_ids": list(barrier_job_ids),
            "submitted_at": utc_now(),
        }
    )
    write_gate_ledger(pipeline_root, ledger)
    return _record_from_payload(key, gates[key])
