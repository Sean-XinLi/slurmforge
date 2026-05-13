"""Lineage payload builders."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ..plans.stage import StageBatchPlan, StageInstancePlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..contracts import InputBinding
from .records import (
    LineageInputSourceRecord,
    LineageStageBatchRef,
    LineageStageInstanceRecord,
    StageBatchLineageRecord,
    TrainEvalPipelineLineageRecord,
)


def _binding_record(
    instance: StageInstancePlan, binding: InputBinding
) -> LineageInputSourceRecord:
    return LineageInputSourceRecord(
        stage_instance_id=instance.stage_instance_id,
        run_id=instance.run_id,
        stage_name=instance.stage_name,
        input_name=binding.input_name,
        source=binding.source,
        expects=binding.expects,
        resolved=binding.resolved,
        resolution=binding.resolution,
    )


def _source_roots_from_bindings(instances: Iterable[StageInstancePlan]) -> list[str]:
    roots: set[str] = set()
    for instance in instances:
        for binding in instance.input_bindings:
            for value in (
                binding.resolution.producer_root,
                binding.resolution.source_root,
            ):
                if value:
                    roots.add(str(Path(value).expanduser().resolve()))
    return sorted(roots)


def build_stage_batch_lineage(batch: StageBatchPlan) -> StageBatchLineageRecord:
    instances = tuple(batch.stage_instances)
    input_sources = tuple(
        _binding_record(instance, binding)
        for instance in instances
        for binding in instance.input_bindings
    )
    return StageBatchLineageRecord(
        root=str(Path(batch.submission_root).resolve()),
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        source_ref=batch.source_ref,
        spec_snapshot_digest=batch.spec_snapshot_digest,
        run_ids=tuple(batch.selected_runs),
        stage_instances=tuple(
            LineageStageInstanceRecord(
                stage_instance_id=instance.stage_instance_id,
                run_id=instance.run_id,
                stage_name=instance.stage_name,
                run_dir_rel=instance.run_dir_rel,
            )
            for instance in instances
        ),
        source_roots=tuple(_source_roots_from_bindings(instances)),
        input_sources=input_sources,
    )


def build_train_eval_pipeline_lineage(
    plan: TrainEvalPipelinePlan,
) -> TrainEvalPipelineLineageRecord:
    return TrainEvalPipelineLineageRecord(
        root=str(Path(plan.root_dir).resolve()),
        pipeline_id=plan.pipeline_id,
        pipeline_kind=plan.pipeline_kind,
        stage_order=tuple(plan.stage_order),
        run_ids=tuple(plan.run_set),
        spec_snapshot_digest=plan.spec_snapshot_digest,
        stage_batches={
            stage_name: LineageStageBatchRef(
                batch_id=batch.batch_id,
                root=str(Path(batch.submission_root).resolve()),
                stage_name=batch.stage_name,
                source_ref=batch.source_ref,
            )
            for stage_name, batch in sorted(plan.stage_batches.items())
        },
        source_roots=tuple(
            str(Path(batch.submission_root).resolve())
            for _stage_name, batch in sorted(plan.stage_batches.items())
        ),
    )
