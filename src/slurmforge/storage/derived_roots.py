from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion, to_jsonable, write_json
from ..plans.sources import SourcedStageBatchPlan, prior_batch_lineage_to_dict


@dataclass(frozen=True)
class ReservedDerivedStageBatchRoot:
    batch_id: str
    root: Path


def reserve_derived_stage_batch_root(
    source_root: Path, base_batch_id: str
) -> ReservedDerivedStageBatchRoot:
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
