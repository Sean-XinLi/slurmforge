from __future__ import annotations

from pathlib import Path

from ..config_contract.workflows import TEMPLATE_TRAIN_EVAL
from . import InitRequest
from .catalog import get_template
from .config_yaml import render_starter_config
from .examples import render_advanced_example

__all__ = ["render_advanced_example", "render_starter_example"]


def render_starter_example(project_root: Path) -> str:
    template = get_template(TEMPLATE_TRAIN_EVAL)
    return render_starter_config(
        TEMPLATE_TRAIN_EVAL,
        template.config_builder(
            InitRequest(template=TEMPLATE_TRAIN_EVAL, output_dir=project_root)
        ),
    )
