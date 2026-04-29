from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Callable

from slurmforge.defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_CONFIG_FILENAME,
    TEMPLATE_TRAIN_EVAL,
    TEMPLATE_TRAIN_ONLY,
)


def init_args(
    root: Path,
    *,
    template: str = TEMPLATE_TRAIN_EVAL,
    force: bool = False,
) -> Namespace:
    return Namespace(
        template=template,
        list_templates=False,
        output=str(root),
        force=force,
    )


def interactive_init_args() -> Namespace:
    return Namespace(
        template=None,
        list_templates=False,
        output=None,
        force=False,
    )


def dry_run_command_for_template(template: str) -> list[str]:
    if template == TEMPLATE_TRAIN_EVAL:
        return ["run"]
    if template == TEMPLATE_TRAIN_ONLY:
        return ["train"]
    return ["eval", "--checkpoint", DEFAULT_CHECKPOINT_PATH]


def bad_template(file_builder: Callable):
    from slurmforge.starter.models import (
        StarterCommandSet,
        StarterReadmePlan,
        StarterTemplate,
    )
    from slurmforge.starter.templates.train_eval import build_config

    return StarterTemplate(
        name="bad-template",
        description="bad",
        config_builder=build_config,
        readme_builder=lambda request: StarterReadmePlan(
            template=request.template,
            commands=StarterCommandSet(
                validate=f"sforge validate --config {DEFAULT_CONFIG_FILENAME}",
                dry_run=f"sforge run --config {DEFAULT_CONFIG_FILENAME} --dry-run=full",
                submit=f"sforge run --config {DEFAULT_CONFIG_FILENAME}",
            ),
        ),
        file_builders=(file_builder,),
    )
