from __future__ import annotations

from typing import Any

from ..models import InitRequest, StarterReadmePlan, StarterTemplate
from ..defaults import TEMPLATE_TRAIN_EVAL
from .fragments import base_config, eval_stage_from_train, train_stage
from .readme import starter_readme_plan
from .scripts import eval_script, train_script


def build_config(_request: InitRequest) -> dict[str, Any]:
    config = base_config()
    config["stages"] = {
        "train": train_stage(),
        "eval": eval_stage_from_train(),
    }
    return config


def build_readme(request: InitRequest) -> StarterReadmePlan:
    config_name = request.output.name
    return starter_readme_plan(
        request,
        dry_run_command=f"sforge run --config {config_name} --dry-run=full",
        submit_command=f"sforge run --config {config_name}",
        editable_fields=(
            "project",
            "experiment",
            "storage.root",
            "stages.train.entry.script",
            "stages.eval.entry.script",
            "stages.*.resources.partition",
            "runtime.executor.python.bin",
            "runtime.user.default.python.bin",
        ),
    )


TRAIN_EVAL_TEMPLATE = StarterTemplate(
    name=TEMPLATE_TRAIN_EVAL,
    description="train stage produces a checkpoint; eval consumes it through upstream_output",
    config_builder=build_config,
    readme_builder=build_readme,
    file_builders=(train_script, eval_script),
)
