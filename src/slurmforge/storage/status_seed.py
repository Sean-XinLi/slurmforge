from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import SchemaVersion, write_json
from ..plans import StageBatchPlan, StageInstancePlan
from ..status import StageStatusRecord, commit_stage_status, write_root_ref
from .paths import input_bindings_path, stage_plan_path


def _run_dir(batch_root: Path, instance: StageInstancePlan) -> Path:
    return batch_root / instance.run_dir_rel


def _bindings_payload(instance: StageInstancePlan) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.INPUT_BINDINGS,
        "stage_instance_id": instance.stage_instance_id,
        "bindings": {
            binding.input_name: {
                "schema_version": binding.schema_version,
                "input_name": binding.input_name,
                "source": binding.source,
                "expects": binding.expects,
                "resolved": binding.resolved,
                "inject": binding.inject,
                "resolution": binding.resolution,
            }
            for binding in instance.input_bindings
        },
    }


def seed_planned_stage_statuses(
    batch: StageBatchPlan,
    batch_root: Path,
    *,
    pipeline_root: Path | None,
) -> None:
    for instance in batch.stage_instances:
        run_dir = _run_dir(batch_root, instance)
        run_dir.mkdir(parents=True, exist_ok=True)
        write_json(stage_plan_path(run_dir), instance)
        write_json(input_bindings_path(run_dir), _bindings_payload(instance))
        write_root_ref(
            run_dir,
            stage_batch_root=batch_root,
            pipeline_root=pipeline_root,
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
