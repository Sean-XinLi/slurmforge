from __future__ import annotations

from typing import Any

from ..models import InitRequest, StarterReadmePlan, StarterTemplate
from ...defaults import DEFAULT_CONFIG_FILENAME, TEMPLATE_TRAIN_EVAL
from .base import base_config
from .readme import starter_readme_plan
from .scripts import eval_script, train_script
from .stage_specs import eval_stage_from_train, train_stage


def build_config(_request: InitRequest) -> dict[str, Any]:
    config = base_config()
    config["stages"] = {
        "train": train_stage(),
        "eval": eval_stage_from_train(),
    }
    return config


def build_readme(request: InitRequest) -> StarterReadmePlan:
    return starter_readme_plan(
        request,
        dry_run_command=f"sforge run --config {DEFAULT_CONFIG_FILENAME} --dry-run=full",
        submit_command=f"sforge run --config {DEFAULT_CONFIG_FILENAME}",
    )


TRAIN_EVAL_TEMPLATE = StarterTemplate(
    name=TEMPLATE_TRAIN_EVAL,
    description="train stage produces a checkpoint; eval consumes it through upstream_output",
    config_builder=build_config,
    readme_builder=build_readme,
    file_builders=(train_script, eval_script),
)
