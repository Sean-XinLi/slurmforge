from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..root_paths import parent_pipeline_root_for_stage_batch
from ..status.machine import commit_stage_status
from ..status.models import StageStatusRecord
from .root_ref import write_root_ref


def seed_planned_stage_statuses(
    batch: StageBatchPlan,
    batch_root: Path | None = None,
    *,
    pipeline_root: Path | None = None,
) -> None:
    resolved_batch_root = (
        Path(batch.submission_root).resolve()
        if batch_root is None
        else Path(batch_root).resolve()
    )
    resolved_pipeline_root = pipeline_root or parent_pipeline_root_for_stage_batch(
        resolved_batch_root
    )
    for instance in batch.stage_instances:
        run_dir = resolved_batch_root / instance.run_dir_rel
        run_dir.mkdir(parents=True, exist_ok=True)
        write_root_ref(
            run_dir,
            stage_batch_root=resolved_batch_root,
            pipeline_root=resolved_pipeline_root,
        )
        commit_stage_status(
            run_dir,
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id=instance.stage_instance_id,
                run_id=instance.run_id,
                stage_name=instance.stage_name,
                state="planned",
            ),
            source="layout",
        )
        (run_dir / "attempts").mkdir(exist_ok=True)


def seed_train_eval_pipeline_statuses(plan: TrainEvalPipelinePlan) -> None:
    pipeline_root = Path(plan.root_dir).resolve()
    for batch in plan.stage_batches.values():
        seed_planned_stage_statuses(
            batch,
            Path(batch.submission_root),
            pipeline_root=pipeline_root,
        )
