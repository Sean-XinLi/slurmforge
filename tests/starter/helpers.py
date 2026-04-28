from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Callable


def init_args(
    root: Path,
    *,
    template: str = "train-eval",
    force: bool = False,
) -> Namespace:
    return Namespace(
        template=template,
        list_templates=False,
        output=str(root / "experiment.yaml"),
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
    if template == "train-eval":
        return ["run"]
    if template == "train-only":
        return ["train"]
    return ["eval", "--checkpoint", "checkpoint.pt"]


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
                validate="sforge validate --config experiment.yaml",
                dry_run="sforge run --config experiment.yaml --dry-run=full",
                submit="sforge run --config experiment.yaml",
            ),
            editable_fields=(),
        ),
        file_builders=(file_builder,),
    )
