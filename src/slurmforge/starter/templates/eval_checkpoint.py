from __future__ import annotations

from typing import Any

from ...config_contract.default_values import DEFAULT_CONFIG_FILENAME
from ...config_contract.registry import default_for
from ...config_contract.workflows import STAGE_EVAL, TEMPLATE_EVAL_CHECKPOINT
from ..models import InitRequest, StarterReadmePlan, StarterTemplate
from .base import base_config
from .readme import starter_readme_plan
from .scripts import checkpoint_file, eval_script
from .stage_specs import eval_stage_external_checkpoint

DEFAULT_CHECKPOINT_PATH = default_for("stages.*.inputs.*.source.path")


def build_config(_request: InitRequest) -> dict[str, Any]:
    config = base_config()
    config["stages"] = {STAGE_EVAL: eval_stage_external_checkpoint()}
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
