from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion
from ..spec import ExperimentSpec
from ..schema import RunDefinition
from .stage import StageBatchPlan


@dataclass(frozen=True)
class StageBatchSource:
    kind: str
    source_root: str = ""
    stage: str = ""
    query: str = ""
    run_ids: tuple[str, ...] = ()
    overrides: tuple[str, ...] = ()
    schema_version: int = SchemaVersion.SOURCE_PLAN


@dataclass(frozen=True)
class SelectedStageRun:
    run_dir: Path
    run: RunDefinition
    stage_instance_id: str
    schema_version: int = SchemaVersion.SOURCE_PLAN


@dataclass(frozen=True)
class PriorBatchLineage:
    source_root: str
    stage: str
    query: str
    selected_run_ids: tuple[str, ...]
    selected_stage_instance_ids: tuple[str, ...]
    overrides: tuple[str, ...] = ()
    derived_batch_id: str = ""
    derived_root: str = ""
    kind: str = "prior_batch"
    schema_version: int = SchemaVersion.SOURCE_PLAN


@dataclass(frozen=True)
class SourcedStageBatchPlan:
    spec: ExperimentSpec
    batch: StageBatchPlan
    source: StageBatchSource
    lineage: PriorBatchLineage
    selected_runs: tuple[SelectedStageRun, ...]
    schema_version: int = SchemaVersion.SOURCE_PLAN


def prior_batch_lineage_to_dict(lineage: PriorBatchLineage) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": lineage.schema_version,
        "kind": lineage.kind,
        "source_root": lineage.source_root,
        "stage": lineage.stage,
        "query": lineage.query,
        "selected_run_ids": list(lineage.selected_run_ids),
        "selected_stage_instance_ids": list(lineage.selected_stage_instance_ids),
        "overrides": list(lineage.overrides),
    }
    if lineage.derived_batch_id:
        payload["derived_batch_id"] = lineage.derived_batch_id
    if lineage.derived_root:
        payload["derived_root"] = lineage.derived_root
    return payload
