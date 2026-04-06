from __future__ import annotations

import copy
import warnings
from typing import Any

from ...config.utils import ensure_dict
from ...utils import read_schema_version
from .model import CURRENT_REPLAY_SCHEMA_VERSION, ReplaySpec


def _warn_schema_version(schema_version: int, *, name: str) -> None:
    warnings.warn(
        f"{name}.schema_version={schema_version} differs from current replay schema "
        f"{CURRENT_REPLAY_SCHEMA_VERSION}; compatibility is not guaranteed",
        stacklevel=3,
    )


def serialize_replay_spec(spec: ReplaySpec) -> dict[str, Any]:
    return {
        "schema_version": int(spec.schema_version),
        "replay_cfg": copy.deepcopy(spec.replay_cfg),
        "planning_root": str(spec.planning_root),
        "source_batch_root": None if spec.source_batch_root in (None, "") else str(spec.source_batch_root),
        "source_run_id": None if spec.source_run_id in (None, "") else str(spec.source_run_id),
        "source_record_path": None if spec.source_record_path in (None, "") else str(spec.source_record_path),
    }


def ensure_replay_spec(value: Any, name: str = "replay_spec") -> ReplaySpec:
    if isinstance(value, ReplaySpec):
        if int(value.schema_version) != CURRENT_REPLAY_SCHEMA_VERSION:
            _warn_schema_version(int(value.schema_version), name="ReplaySpec")
        return value
    data = ensure_dict(value, name)
    schema_version = read_schema_version(data, default=0)
    if schema_version != CURRENT_REPLAY_SCHEMA_VERSION:
        _warn_schema_version(schema_version, name=name)
    return ReplaySpec(
        schema_version=schema_version,
        replay_cfg=copy.deepcopy(ensure_dict(data.get("replay_cfg"), f"{name}.replay_cfg")),
        planning_root=str(data.get("planning_root", "") or ""),
        source_batch_root=None if data.get("source_batch_root") in (None, "") else str(data.get("source_batch_root")),
        source_run_id=None if data.get("source_run_id") in (None, "") else str(data.get("source_run_id")),
        source_record_path=None if data.get("source_record_path") in (None, "") else str(data.get("source_record_path")),
    )
