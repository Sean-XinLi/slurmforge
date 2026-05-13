from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from ..contracts import InputResolution, InputSource, ResolvedInput
from ..contracts import input_source_from_dict, resolved_input_from_dict
from ..contracts import input_resolution_from_dict
from ..errors import RecordContractError
from ..io import SchemaVersion, require_schema, to_jsonable
from ..record_fields import (
    required_object,
    required_object_array,
    required_record,
    required_string,
    required_string_array,
)

STAGE_BATCH_LINEAGE_KIND = "stage_batch_lineage"
TRAIN_EVAL_PIPELINE_LINEAGE_KIND = "train_eval_pipeline_lineage"


@dataclass(frozen=True)
class LineageStageInstanceRecord:
    stage_instance_id: str
    run_id: str
    stage_name: str
    run_dir_rel: str


@dataclass(frozen=True)
class LineageInputSourceRecord:
    stage_instance_id: str
    run_id: str
    stage_name: str
    input_name: str
    source: InputSource
    expects: str
    resolved: ResolvedInput
    resolution: InputResolution


@dataclass(frozen=True)
class StageBatchLineageRecord:
    root: str
    batch_id: str
    stage_name: str
    source_ref: str
    spec_snapshot_digest: str
    run_ids: tuple[str, ...]
    stage_instances: tuple[LineageStageInstanceRecord, ...]
    source_roots: tuple[str, ...]
    input_sources: tuple[LineageInputSourceRecord, ...]
    kind: Literal["stage_batch_lineage"] = STAGE_BATCH_LINEAGE_KIND
    schema_version: int = SchemaVersion.LINEAGE


@dataclass(frozen=True)
class LineageStageBatchRef:
    batch_id: str
    root: str
    stage_name: str
    source_ref: str


@dataclass(frozen=True)
class TrainEvalPipelineLineageRecord:
    root: str
    pipeline_id: str
    pipeline_kind: str
    stage_order: tuple[str, ...]
    run_ids: tuple[str, ...]
    spec_snapshot_digest: str
    stage_batches: dict[str, LineageStageBatchRef]
    source_roots: tuple[str, ...]
    kind: Literal["train_eval_pipeline_lineage"] = TRAIN_EVAL_PIPELINE_LINEAGE_KIND
    schema_version: int = SchemaVersion.LINEAGE


LineageIndexRecord = StageBatchLineageRecord | TrainEvalPipelineLineageRecord


def lineage_index_from_dict(payload: dict[str, Any]) -> LineageIndexRecord:
    version = require_schema(payload, name="lineage_index", version=SchemaVersion.LINEAGE)
    kind = required_string(payload, "kind", label="lineage_index", non_empty=True)
    if kind == STAGE_BATCH_LINEAGE_KIND:
        return _stage_batch_lineage_from_dict(payload, schema_version=version)
    if kind == TRAIN_EVAL_PIPELINE_LINEAGE_KIND:
        return _train_eval_pipeline_lineage_from_dict(payload, schema_version=version)
    raise RecordContractError(f"Unsupported lineage index kind: {kind}")


def lineage_index_to_dict(record: LineageIndexRecord) -> dict[str, Any]:
    payload = to_jsonable(record)
    if not isinstance(payload, dict):
        raise RecordContractError("lineage index must serialize to an object")
    return payload


def _stage_batch_lineage_from_dict(
    payload: dict[str, Any], *, schema_version: int
) -> StageBatchLineageRecord:
    return StageBatchLineageRecord(
        root=required_string(payload, "root", label="lineage_index", non_empty=True),
        batch_id=required_string(
            payload, "batch_id", label="lineage_index", non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label="lineage_index", non_empty=True
        ),
        source_ref=required_string(
            payload, "source_ref", label="lineage_index", non_empty=True
        ),
        spec_snapshot_digest=required_string(
            payload, "spec_snapshot_digest", label="lineage_index"
        ),
        run_ids=required_string_array(payload, "run_ids", label="lineage_index"),
        stage_instances=tuple(
            _lineage_stage_instance_from_dict(item)
            for item in required_object_array(
                payload, "stage_instances", label="lineage_index"
            )
        ),
        source_roots=required_string_array(
            payload, "source_roots", label="lineage_index"
        ),
        input_sources=tuple(
            _lineage_input_source_from_dict(item)
            for item in required_object_array(
                payload, "input_sources", label="lineage_index"
            )
        ),
        schema_version=schema_version,
    )


def _train_eval_pipeline_lineage_from_dict(
    payload: dict[str, Any], *, schema_version: int
) -> TrainEvalPipelineLineageRecord:
    return TrainEvalPipelineLineageRecord(
        root=required_string(payload, "root", label="lineage_index", non_empty=True),
        pipeline_id=required_string(
            payload, "pipeline_id", label="lineage_index", non_empty=True
        ),
        pipeline_kind=required_string(
            payload, "pipeline_kind", label="lineage_index", non_empty=True
        ),
        stage_order=required_string_array(
            payload, "stage_order", label="lineage_index"
        ),
        run_ids=required_string_array(payload, "run_ids", label="lineage_index"),
        spec_snapshot_digest=required_string(
            payload, "spec_snapshot_digest", label="lineage_index"
        ),
        stage_batches=_lineage_stage_batches_from_dict(
            required_object(payload, "stage_batches", label="lineage_index")
        ),
        source_roots=required_string_array(
            payload, "source_roots", label="lineage_index"
        ),
        schema_version=schema_version,
    )


def _lineage_stage_instance_from_dict(
    payload: dict[str, Any],
) -> LineageStageInstanceRecord:
    label = "lineage_index.stage_instances"
    return LineageStageInstanceRecord(
        stage_instance_id=required_string(
            payload, "stage_instance_id", label=label, non_empty=True
        ),
        run_id=required_string(payload, "run_id", label=label, non_empty=True),
        stage_name=required_string(payload, "stage_name", label=label, non_empty=True),
        run_dir_rel=required_string(
            payload, "run_dir_rel", label=label, non_empty=True
        ),
    )


def _lineage_stage_batches_from_dict(
    payload: dict[str, Any],
) -> dict[str, LineageStageBatchRef]:
    batches: dict[str, LineageStageBatchRef] = {}
    for stage_name, raw_batch in payload.items():
        key = _stage_batch_key(stage_name)
        batch = required_record(raw_batch, f"lineage_index.stage_batches.{key}")
        batch_ref = LineageStageBatchRef(
            batch_id=required_string(
                batch, "batch_id", label="lineage_index.stage_batches", non_empty=True
            ),
            root=required_string(
                batch, "root", label="lineage_index.stage_batches", non_empty=True
            ),
            stage_name=required_string(
                batch,
                "stage_name",
                label="lineage_index.stage_batches",
                non_empty=True,
            ),
            source_ref=required_string(
                batch,
                "source_ref",
                label="lineage_index.stage_batches",
                non_empty=True,
            ),
        )
        if batch_ref.stage_name != key:
            raise RecordContractError(
                f"lineage_index.stage_batches key `{key}` does not match `{batch_ref.stage_name}`"
            )
        batches[key] = batch_ref
    return batches


def _stage_batch_key(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise RecordContractError(
            "lineage_index.stage_batches keys must be non-empty strings"
        )
    return value


def _lineage_input_source_from_dict(
    payload: dict[str, Any],
) -> LineageInputSourceRecord:
    label = "lineage_index.input_sources"
    source = input_source_from_dict(required_object(payload, "source", label=label))
    resolved = resolved_input_from_dict(
        required_object(payload, "resolved", label=label)
    )
    resolution = input_resolution_from_dict(
        required_object(payload, "resolution", label=label)
    )
    return LineageInputSourceRecord(
        stage_instance_id=required_string(
            payload, "stage_instance_id", label=label, non_empty=True
        ),
        run_id=required_string(payload, "run_id", label=label, non_empty=True),
        stage_name=required_string(payload, "stage_name", label=label, non_empty=True),
        input_name=required_string(
            payload, "input_name", label=label, non_empty=True
        ),
        source=source,
        expects=required_string(payload, "expects", label=label, non_empty=True),
        resolved=resolved,
        resolution=resolution,
    )
