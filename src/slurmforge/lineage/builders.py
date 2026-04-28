"""Lineage payload builders."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..io import SchemaVersion
from ..plans import TrainEvalPipelinePlan, StageBatchPlan, StageInstancePlan
from ..contracts import InputBinding


def _binding_record(instance: StageInstancePlan, binding: InputBinding) -> dict[str, Any]:
    return {
        "stage_instance_id": instance.stage_instance_id,
        "run_id": instance.run_id,
        "stage_name": instance.stage_name,
        "input_name": binding.input_name,
        "source": binding.source,
        "expects": binding.expects,
        "resolved": binding.resolved,
        "resolution": dict(binding.resolution),
    }


def _source_roots_from_bindings(instances: Iterable[StageInstancePlan]) -> list[str]:
    roots: set[str] = set()
    for instance in instances:
        for binding in instance.input_bindings:
            resolution = dict(binding.resolution)
            for key in ("producer_root", "source_root"):
                value = resolution.get(key)
                if value:
                    roots.add(str(Path(str(value)).expanduser().resolve()))
    return sorted(roots)


def build_stage_batch_lineage(batch: StageBatchPlan) -> dict[str, Any]:
    instances = tuple(batch.stage_instances)
    input_sources = [
        _binding_record(instance, binding)
        for instance in instances
        for binding in instance.input_bindings
    ]
    return {
        "schema_version": SchemaVersion.LINEAGE,
        "kind": "stage_batch_lineage",
        "root": str(Path(batch.submission_root).resolve()),
        "batch_id": batch.batch_id,
        "stage_name": batch.stage_name,
        "source_ref": batch.source_ref,
        "spec_snapshot_digest": batch.spec_snapshot_digest,
        "run_ids": list(batch.selected_runs),
        "stage_instances": [
            {
                "stage_instance_id": instance.stage_instance_id,
                "run_id": instance.run_id,
                "stage_name": instance.stage_name,
                "run_dir_rel": instance.run_dir_rel,
            }
            for instance in instances
        ],
        "source_roots": _source_roots_from_bindings(instances),
        "input_sources": input_sources,
    }


def build_train_eval_pipeline_lineage(plan: TrainEvalPipelinePlan) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.LINEAGE,
        "kind": "train_eval_pipeline_lineage",
        "root": str(Path(plan.root_dir).resolve()),
        "pipeline_id": plan.pipeline_id,
        "pipeline_kind": plan.pipeline_kind,
        "stage_order": list(plan.stage_order),
        "run_ids": list(plan.run_set),
        "spec_snapshot_digest": plan.spec_snapshot_digest,
        "stage_batches": {
            stage_name: {
                "batch_id": batch.batch_id,
                "root": str(Path(batch.submission_root).resolve()),
                "stage_name": batch.stage_name,
                "source_ref": batch.source_ref,
            }
            for stage_name, batch in sorted(plan.stage_batches.items())
        },
        "source_roots": [
            str(Path(batch.submission_root).resolve())
            for _stage_name, batch in sorted(plan.stage_batches.items())
        ],
    }
