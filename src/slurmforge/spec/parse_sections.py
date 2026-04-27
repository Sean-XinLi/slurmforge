from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigContractError
from ..io import content_digest
from ..overrides import deep_set, parse_override
from .models import ExperimentSpec, StorageSpec
from .parse_common import require_mapping
from .parse_dispatch import parse_dispatch
from .parse_notifications import parse_notifications
from .parse_resources import parse_hardware, parse_sizing
from .parse_runs import parse_runs
from .parse_runtime import parse_environments, parse_orchestration, parse_runtime
from .parse_stages import parse_artifact_store, parse_stage


_ALLOWED_TOP_LEVEL_KEYS = {
    "project",
    "experiment",
    "storage",
    "hardware",
    "environments",
    "sizing",
    "runs",
    "notifications",
    "runtime",
    "artifact_store",
    "stages",
    "dispatch",
    "orchestration",
}


def load_raw_config(config_path: Path, cli_overrides: tuple[str, ...] = ()) -> dict[str, Any]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigContractError(f"Config must be a YAML mapping: {config_path}")
    raw = copy.deepcopy(raw)
    for override in cli_overrides:
        key, value = parse_override(override)
        deep_set(raw, key, value)
    return raw


def parse_experiment_spec(
    raw: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path,
    forced_digest: str | None = None,
) -> ExperimentSpec:
    if "stages" not in raw:
        raise ConfigContractError("Configs must use top-level `stages`")
    if "matrix" in raw:
        raise ConfigContractError("Top-level `matrix` is not supported; use top-level `runs`")
    if "eval" in raw or "run" in raw or "model" in raw:
        raise ConfigContractError("Top-level `model`, `run`, and `eval` are legacy fields; use `stages.<name>`")
    unknown_top_level = sorted(set(raw) - _ALLOWED_TOP_LEVEL_KEYS)
    if unknown_top_level:
        joined = ", ".join(str(item) for item in unknown_top_level)
        raise ConfigContractError(f"Unsupported top-level keys: {joined}")
    project = raw.get("project")
    experiment = raw.get("experiment")
    if project in (None, ""):
        raise ConfigContractError("`project` is required")
    if experiment in (None, ""):
        raise ConfigContractError("`experiment` is required")
    storage = require_mapping(raw.get("storage"), "storage")
    storage_root = storage.get("root")
    if storage_root in (None, ""):
        raise ConfigContractError("`storage.root` is required")
    stages_raw = require_mapping(raw.get("stages"), "stages")
    unknown_stages = sorted(set(stages_raw) - {"train", "eval"})
    if unknown_stages:
        joined = ", ".join(str(item) for item in unknown_stages)
        raise ConfigContractError(f"Unsupported stage keys: {joined}. Stage-batch v1 only supports train and eval")
    stages = {str(name): parse_stage(str(name), stage_raw) for name, stage_raw in stages_raw.items()}
    enabled = {name: stage for name, stage in stages.items() if stage.enabled}
    if not enabled:
        raise ConfigContractError("At least one stage must be enabled")
    if "eval" in enabled and "train" not in enabled and enabled["eval"].depends_on:
        raise ConfigContractError("`stages.eval.depends_on` requires enabled `stages.train`")
    for name, stage in enabled.items():
        for dep in stage.depends_on:
            if dep not in enabled:
                raise ConfigContractError(f"`stages.{name}.depends_on` references disabled or unknown stage `{dep}`")
    spec = ExperimentSpec(
        project=str(project),
        experiment=str(experiment),
        storage=StorageSpec(root=str(storage_root)),
        hardware=parse_hardware(raw.get("hardware")),
        environments=parse_environments(raw.get("environments")),
        sizing=parse_sizing(raw.get("sizing")),
        runs=parse_runs(raw.get("runs")),
        notifications=parse_notifications(raw.get("notifications")),
        runtime=parse_runtime(raw.get("runtime")),
        artifact_store=parse_artifact_store(raw.get("artifact_store")),
        stages=stages,
        dispatch=parse_dispatch(raw.get("dispatch")),
        orchestration=parse_orchestration(raw.get("orchestration")),
        project_root=project_root.resolve(),
        config_path=config_path.resolve(),
        spec_snapshot_digest=forced_digest or content_digest(raw),
        raw=copy.deepcopy(raw),
    )
    spec.stage_order()
    return spec
