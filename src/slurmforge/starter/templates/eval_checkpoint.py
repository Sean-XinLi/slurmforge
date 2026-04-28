from __future__ import annotations

from typing import Any

from ..models import InitRequest, StarterReadmePlan, StarterTemplate
from ..defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_CONFIG_FILENAME,
    TEMPLATE_EVAL_CHECKPOINT,
)
from .base import base_config
from .readme import starter_readme_plan
from .scripts import checkpoint_file, eval_script
from .stage_specs import eval_stage_external_checkpoint


def build_config(_request: InitRequest) -> dict[str, Any]:
    config = base_config()
    config["stages"] = {"eval": eval_stage_external_checkpoint()}
    return config


def build_readme(request: InitRequest) -> StarterReadmePlan:
    return starter_readme_plan(
        request,
        dry_run_command=(
            f"sforge eval --config {DEFAULT_CONFIG_FILENAME} "
            f"--checkpoint {DEFAULT_CHECKPOINT_PATH} --dry-run=full"
        ),
        submit_command=(
            f"sforge eval --config {DEFAULT_CONFIG_FILENAME} "
            f"--checkpoint {DEFAULT_CHECKPOINT_PATH}"
        ),
        editable_fields=(
            "project",
            "experiment",
            "storage.root",
            "stages.eval.entry.script",
            "stages.eval.resources.partition",
            "runtime.executor.python.bin",
            "runtime.user.default.python.bin",
        ),
        notes=(
            f"`{DEFAULT_CHECKPOINT_PATH}` is a sample input file generated with this template.",
            "Relative `--checkpoint` paths are resolved from the config directory.",
        ),
    )


EVAL_CHECKPOINT_TEMPLATE = StarterTemplate(
    name=TEMPLATE_EVAL_CHECKPOINT,
    description="eval stage that consumes an explicit external checkpoint path",
    config_builder=build_config,
    readme_builder=build_readme,
    file_builders=(eval_script, checkpoint_file),
)
