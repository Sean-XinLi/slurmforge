from __future__ import annotations

from pathlib import Path
from typing import Any

from ....model_support.catalog import serialize_resolved_model_catalog
from ..constants import REPLAY_MODEL_CATALOG_KEY
from ..models.experiment import ExperimentSpec
from ..replay_payload import canonicalize_replay_payload
from ..runtime import (
    serialize_artifacts_config,
    serialize_cluster_config,
    serialize_dispatch_config,
    serialize_env_config,
    serialize_launcher_config,
    serialize_notify_config,
    serialize_resources_config,
    serialize_validation_config,
)
from .eval import serialize_eval_config
from .model import serialize_model_config
from .output import serialize_output_config
from .run import serialize_run_config


def serialize_experiment_spec(spec: ExperimentSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "project": spec.project,
        "experiment_name": spec.experiment_name,
        "run": serialize_run_config(spec.run),
        "launcher": serialize_launcher_config(spec.launcher),
        "cluster": serialize_cluster_config(spec.cluster),
        "env": serialize_env_config(spec.env),
        "resources": serialize_resources_config(spec.resources),
        "dispatch": serialize_dispatch_config(spec.dispatch),
        "artifacts": serialize_artifacts_config(spec.artifacts),
        "eval": serialize_eval_config(spec.eval),
        "output": serialize_output_config(spec.output),
        "notify": serialize_notify_config(spec.notify),
        "validation": serialize_validation_config(spec.validation),
        REPLAY_MODEL_CATALOG_KEY: serialize_resolved_model_catalog(spec.model_catalog),
    }
    if spec.model is not None:
        payload["model"] = serialize_model_config(spec.model)
    return payload


def serialize_replay_experiment_spec(
    spec: ExperimentSpec,
    *,
    project_root: Path,
) -> dict[str, Any]:
    return canonicalize_replay_payload(
        serialize_experiment_spec(spec),
        project_root=project_root,
    )
