from __future__ import annotations

import copy
from typing import Any

from ....errors import ConfigContractError
from ...config.codecs import ensure_eval_train_outputs_config, serialize_eval_train_outputs_config
from ...config.normalize import ensure_artifacts_config, ensure_cluster_config, ensure_env_config
from ...config.runtime import serialize_artifacts_config, serialize_cluster_config, serialize_env_config
from ...config.utils import ensure_dict
from ...planning.contracts import (
    ensure_plan_diagnostic,
    ensure_stage_execution_plan,
    serialize_plan_diagnostic,
    serialize_stage_execution_plan,
)
from .array_assignment import ensure_array_assignment, serialize_array_assignment
from ..models.dispatch import DispatchInfo
from ..models.run_plan import RunPlan
from .metadata import ensure_generated_by, serialize_generated_by


def serialize_dispatch_info(dispatch: DispatchInfo) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sbatch_path_rel": None if dispatch.sbatch_path_rel is None else str(dispatch.sbatch_path_rel),
        "record_path_rel": None if dispatch.record_path_rel is None else str(dispatch.record_path_rel),
        "array_group": None if dispatch.array_group is None else int(dispatch.array_group),
        "array_task_index": None if dispatch.array_task_index is None else int(dispatch.array_task_index),
        "array_assignment": None,
    }
    if dispatch.array_assignment is not None:
        payload["array_assignment"] = serialize_array_assignment(dispatch.array_assignment)
    return payload


def deserialize_dispatch_info(value: Any) -> DispatchInfo:
    data = ensure_dict(value, "dispatch")
    return DispatchInfo(
        sbatch_path="",
        sbatch_path_rel=None if data.get("sbatch_path_rel") in (None, "") else str(data.get("sbatch_path_rel")),
        record_path=None,
        record_path_rel=None if data.get("record_path_rel") in (None, "") else str(data.get("record_path_rel")),
        array_group=None if data.get("array_group") is None else int(data.get("array_group")),
        array_task_index=None if data.get("array_task_index") is None else int(data.get("array_task_index")),
        array_assignment=None
        if data.get("array_assignment") is None
        else ensure_array_assignment(data.get("array_assignment")),
    )


def serialize_run_plan(plan: RunPlan) -> dict[str, Any]:
    return {
        "run_index": int(plan.run_index),
        "total_runs": int(plan.total_runs),
        "run_id": str(plan.run_id),
        "generated_by": serialize_generated_by(plan.generated_by),
        "project": str(plan.project),
        "experiment_name": str(plan.experiment_name),
        "model_name": str(plan.model_name),
        "train_mode": str(plan.train_mode),
        "train_stage": serialize_stage_execution_plan(plan.train_stage),
        "eval_stage": None if plan.eval_stage is None else serialize_stage_execution_plan(plan.eval_stage),
        "eval_train_outputs": serialize_eval_train_outputs_config(plan.eval_train_outputs),
        "cluster": serialize_cluster_config(plan.cluster),
        "env": serialize_env_config(plan.env),
        "run_dir_rel": None if plan.run_dir_rel is None else str(plan.run_dir_rel),
        "dispatch": serialize_dispatch_info(plan.dispatch),
        "artifacts": serialize_artifacts_config(plan.artifacts),
        "sweep_case_name": None if plan.sweep_case_name in (None, "") else str(plan.sweep_case_name),
        "sweep_assignments": copy.deepcopy(dict(plan.sweep_assignments or {})),
        "planning_diagnostics": [serialize_plan_diagnostic(item) for item in plan.planning_diagnostics],
    }


def deserialize_run_plan(payload: dict[str, Any]) -> RunPlan:
    if not isinstance(payload, dict):
        raise TypeError("run record must be a mapping")
    if payload.get("run_dir_rel") in (None, ""):
        raise ConfigContractError("run record must include run_dir_rel")
    return RunPlan(
        run_index=int(payload["run_index"]),
        total_runs=int(payload["total_runs"]),
        run_id=str(payload["run_id"]),
        generated_by=ensure_generated_by(payload.get("generated_by")),
        project=str(payload["project"]),
        experiment_name=str(payload["experiment_name"]),
        model_name=str(payload["model_name"]),
        train_mode=str(payload["train_mode"]),
        train_stage=ensure_stage_execution_plan(payload.get("train_stage"), name="train_stage"),
        eval_stage=None if payload.get("eval_stage") is None else ensure_stage_execution_plan(payload.get("eval_stage"), name="eval_stage"),
        eval_train_outputs=ensure_eval_train_outputs_config(
            payload.get("eval_train_outputs"),
            name="eval_train_outputs",
        ),
        cluster=ensure_cluster_config(payload.get("cluster")),
        env=ensure_env_config(payload.get("env")),
        run_dir="",
        run_dir_rel=None if payload.get("run_dir_rel") in (None, "") else str(payload.get("run_dir_rel")),
        dispatch=deserialize_dispatch_info(payload.get("dispatch")),
        artifacts=ensure_artifacts_config(payload.get("artifacts")),
        sweep_case_name=None if payload.get("sweep_case_name") in (None, "") else str(payload.get("sweep_case_name")),
        sweep_assignments=copy.deepcopy(ensure_dict(payload.get("sweep_assignments"), "sweep_assignments")),
        planning_diagnostics=tuple(
            ensure_plan_diagnostic(item, name="planning_diagnostics[]")
            for item in list(payload.get("planning_diagnostics") or [])
        ),
    )
