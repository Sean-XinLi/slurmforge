from __future__ import annotations

from ...errors import InternalCompilerError
from ...storage.contracts import PlanningStore
from ...storage.models import MaterializedBatchBundle
from ..planning import PlannedBatch
from .context import MaterializationResult
from .layout import resolve_materialization_layout


def materialize_batch(
    *,
    planned_batch: PlannedBatch,
    planning_store: PlanningStore,
) -> MaterializationResult:
    planned_runs = tuple(planned_batch.planned_runs)
    if not planned_runs:
        raise InternalCompilerError("materialize_batch requires at least one planned run")

    bundle = MaterializedBatchBundle(
        identity=planned_batch.identity,
        planned_runs=planned_runs,
        planning_diagnostics=(),
        notify_cfg=planned_batch.notify_cfg,
        submit_dependencies=dict(planned_batch.submit_dependencies),
        manifest_extras=dict(planned_batch.manifest_extras),
        storage_config=planned_batch.storage_config,
        max_available_gpus=planned_batch.max_available_gpus,
        dispatch_cfg=planned_batch.dispatch_cfg,
        gpu_budget_plan=planned_batch.gpu_budget_plan,
    )

    layout = resolve_materialization_layout(planned_batch.batch_root)
    array_groups_meta = planning_store.persist_materialized_batch(bundle)

    return MaterializationResult(
        submit_script=layout.submit_script,
        manifest_path=layout.manifest_path,
        array_groups_meta=array_groups_meta,
    )
