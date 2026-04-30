from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import sys
from typing import Callable

import yaml

from slurmforge.config_contract.defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_CONFIG_FILENAME,
)
from slurmforge.config_contract.workflows import (
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


def use_current_python_for_dry_run(config_path: Path) -> None:
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["runtime"]["executor"]["python"]["bin"] = sys.executable
    payload["runtime"]["user"]["default"]["python"]["bin"] = sys.executable
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


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
