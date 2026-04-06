from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path

from ....errors import PlanningError
from ....model_support.catalog import ModelSpec, ResolvedModelCatalog, resolve_model_spec as resolve_catalog_model_spec
from ...config.api import ExperimentSpec, serialize_model_config
from .context import PreparedTrainPlan


def default_port_offset(batch_root: Path) -> int:
    return int(hashlib.sha1(str(batch_root).encode("utf-8")).hexdigest()[:6], 16) % 1000


def resolve_model_spec(
    model_catalog: ResolvedModelCatalog,
    model_cfg,
    *,
    project_root: Path,
) -> ModelSpec:
    if model_cfg is None:
        return ModelSpec(
            name="external",
            script=None,
            yaml_path=None,
            ddp_supported=True,
            ddp_required=False,
            estimator_profile="default",
        )
    return resolve_catalog_model_spec(model_catalog, serialize_model_config(model_cfg), project_root=project_root)


def prepare_train_plan(
    spec: ExperimentSpec,
    *,
    project_root: Path,
    batch_root: Path,
) -> PreparedTrainPlan:
    train_mode = spec.run.mode

    if train_mode == "model_cli":
        if spec.model is None:
            raise PlanningError("model_cli mode requires model config")
        model_spec = resolve_catalog_model_spec(
            spec.model_catalog,
            serialize_model_config(spec.model),
            project_root=project_root,
        )
        if model_spec.script is None or not model_spec.script.exists():
            raise FileNotFoundError(f"Model script not found: {model_spec.script}")
    else:
        model_spec = resolve_model_spec(
            spec.model_catalog,
            spec.model,
            project_root=project_root,
        )

    launcher_cfg = spec.launcher
    if launcher_cfg.distributed.port_offset in (None, ""):
        launcher_cfg = replace(
            launcher_cfg,
            distributed=replace(launcher_cfg.distributed, port_offset=default_port_offset(batch_root)),
        )

    run_args = copy.deepcopy(spec.run.args)
    model_overrides = copy.deepcopy(spec.run.model_overrides)
    resume_from_checkpoint = spec.run.resume_from_checkpoint
    if resume_from_checkpoint not in (None, "") and train_mode == "model_cli":
        run_args.setdefault("resume_from_checkpoint", str(resume_from_checkpoint))
    if train_mode == "model_cli" and model_overrides and "model_overrides_json" not in run_args:
        run_args["model_overrides_json"] = json.dumps(model_overrides, sort_keys=True, separators=(",", ":"))

    return PreparedTrainPlan(
        spec=spec,
        project_root=project_root,
        train_mode=train_mode,
        model_spec=model_spec,
        launcher_cfg=launcher_cfg,
        cluster_cfg=spec.cluster,
        cluster_nodes_explicit=spec.hints.cluster_nodes_explicit,
        cluster_gpus_per_node_explicit=spec.hints.cluster_gpus_per_node_explicit,
        launcher_nproc_per_node_explicit=spec.hints.launcher_nproc_per_node_explicit,
        env_cfg=spec.env,
        resources_cfg=spec.resources,
        artifacts_cfg=spec.artifacts,
        validation_cfg=spec.validation,
        eval_spec=spec.eval,
        run_args=run_args,
        model_overrides=model_overrides,
    )
