from __future__ import annotations

from typing import Any

from ..models import InitRequest, StarterReadmePlan, StarterTemplate
from ..defaults import TEMPLATE_TRAIN_ONLY
from .base import base_config
from .readme import starter_readme_plan
from .scripts import train_script
from .stages import train_stage


def build_config(_request: InitRequest) -> dict[str, Any]:
    config = base_config()
    config["stages"] = {"train": train_stage()}
    return config


def build_readme(request: InitRequest) -> StarterReadmePlan:
    config_name = request.output.name
    return starter_readme_plan(
        request,
        dry_run_command=f"sforge train --config {config_name} --dry-run=full",
        submit_command=f"sforge train --config {config_name}",
        editable_fields=(
            "project",
            "experiment",
            "storage.root",
            "stages.train.entry.script",
            "stages.train.resources.partition",
            "runtime.executor.python.bin",
            "runtime.user.default.python.bin",
        ),
    )


TRAIN_ONLY_TEMPLATE = StarterTemplate(
    name=TEMPLATE_TRAIN_ONLY,
    description="single train stage with a checkpoint output",
    config_builder=build_config,
    readme_builder=build_readme,
    file_builders=(train_script,),
)
