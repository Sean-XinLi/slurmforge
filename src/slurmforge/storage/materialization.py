from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from ..io import SchemaVersion, read_json, require_schema, to_jsonable, utc_now, write_json
from ..plans.sources import SourcedStageBatchPlan
from ..plans.sources import prior_batch_lineage_to_dict


@dataclass(frozen=True)
class MaterializationStatusRecord:
    schema_version: int
    batch_id: str
    stage_name: str
    state: str
    failure_class: str | None = None
    reason: str = ""
    verified_at: str = ""
    submit_manifest_path: str = ""


def materialization_status_path(batch_root: Path) -> Path:
    return batch_root / "materialization_status.json"


def materialization_status_from_dict(payload: dict) -> MaterializationStatusRecord:
    version = require_schema(payload, name="materialization_status", version=SchemaVersion.MATERIALIZATION_STATUS)
    return MaterializationStatusRecord(
        schema_version=version,
        batch_id=str(payload["batch_id"]),
        stage_name=str(payload["stage_name"]),
        state=str(payload.get("state") or "planned"),
        failure_class=None if payload.get("failure_class") in (None, "") else str(payload.get("failure_class")),
        reason=str(payload.get("reason") or ""),
        verified_at=str(payload.get("verified_at") or ""),
        submit_manifest_path=str(payload.get("submit_manifest_path") or ""),
    )


def read_materialization_status(batch_root: Path) -> MaterializationStatusRecord | None:
    path = materialization_status_path(batch_root)
    if not path.exists():
        return None
    return materialization_status_from_dict(read_json(path))


def write_materialization_status(
    batch_root: Path,
    *,
    batch_id: str,
    stage_name: str,
    state: str,
    failure_class: str | None = None,
    reason: str = "",
    submit_manifest_path: str = "",
    verified_at: str | None = None,
) -> MaterializationStatusRecord:
    record = MaterializationStatusRecord(
        schema_version=SchemaVersion.MATERIALIZATION_STATUS,
        batch_id=batch_id,
        stage_name=stage_name,
        state=state,
        failure_class=failure_class,
        reason=reason,
        verified_at=utc_now() if verified_at is None and state in {"verifying_inputs", "ready", "blocked"} else (verified_at or ""),
        submit_manifest_path=submit_manifest_path,
    )
    write_json(materialization_status_path(batch_root), record)
    return record


@dataclass(frozen=True)
class ReservedDerivedStageBatchRoot:
    batch_id: str
    root: Path


def reserve_derived_stage_batch_root(source_root: Path, base_batch_id: str) -> ReservedDerivedStageBatchRoot:
    derived_root = Path(source_root).resolve() / "derived_batches"
    derived_root.mkdir(parents=True, exist_ok=True)
    for index in range(1, 10000):
        batch_id = base_batch_id if index == 1 else f"{base_batch_id}_r{index:04d}"
        root = derived_root / batch_id
        try:
            root.mkdir(parents=False, exist_ok=False)
        except FileExistsError:
            continue
        return ReservedDerivedStageBatchRoot(batch_id=batch_id, root=root.resolve())
    raise RuntimeError(f"could not reserve a derived stage batch root under {derived_root}")


def _source_plan_payload(plan: SourcedStageBatchPlan) -> dict[str, object]:
    lineage = prior_batch_lineage_to_dict(plan.lineage)
    return {
        "schema_version": SchemaVersion.SOURCE_PLAN,
        "kind": "sourced_stage_batch_plan",
        "source": to_jsonable(plan.source),
        "source_root": str(plan.lineage.source_root),
        "stage": str(plan.lineage.stage),
        "query": str(plan.lineage.query),
        "overrides": list(plan.lineage.overrides),
        "selected_run_ids": list(plan.lineage.selected_run_ids),
        "selected_stage_instance_ids": list(plan.lineage.selected_stage_instance_ids),
        "lineage": lineage,
        "batch": to_jsonable(plan.batch),
    }


def write_source_contract(plan: SourcedStageBatchPlan) -> None:
    root = Path(plan.batch.submission_root)
    write_json(root / "source_plan.json", _source_plan_payload(plan))
    write_json(root / "source_lineage.json", prior_batch_lineage_to_dict(plan.lineage))


def materialize_sourced_stage_batch_plan(plan: SourcedStageBatchPlan) -> SourcedStageBatchPlan:
    from .batch_layout import write_stage_batch_layout

    source_root = Path(plan.lineage.source_root)
    reserved = reserve_derived_stage_batch_root(source_root, plan.batch.batch_id)
    lineage = replace(
        plan.lineage,
        derived_batch_id=reserved.batch_id,
        derived_root=str(reserved.root),
    )
    batch = replace(plan.batch, batch_id=reserved.batch_id, submission_root=str(reserved.root))
    concrete = replace(plan, batch=batch, lineage=lineage)
    write_source_contract(concrete)
    write_stage_batch_layout(concrete.batch, spec_snapshot=concrete.spec_snapshot)
    return concrete
