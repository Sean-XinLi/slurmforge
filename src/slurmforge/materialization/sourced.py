from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ..plans.sources import SourcedStageBatchPlan
from ..storage.derived_roots import (
    reserve_derived_stage_batch_root,
    write_source_contract,
)
from .stage_batch import materialize_stage_batch


def materialize_sourced_stage_batch(
    plan: SourcedStageBatchPlan,
) -> SourcedStageBatchPlan:
    source_root = Path(plan.lineage.source_root)
    reserved = reserve_derived_stage_batch_root(source_root, plan.batch.batch_id)
    lineage = replace(
        plan.lineage,
        derived_batch_id=reserved.batch_id,
        derived_root=str(reserved.root),
    )
    batch = replace(
        plan.batch,
        batch_id=reserved.batch_id,
        submission_root=str(reserved.root),
    )
    concrete = replace(plan, batch=batch, lineage=lineage)
    write_source_contract(concrete)
    materialize_stage_batch(concrete.batch, spec_snapshot=concrete.spec_snapshot)
    return concrete
