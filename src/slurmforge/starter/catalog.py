from __future__ import annotations

from ..errors import UsageError
from ..defaults import TEMPLATE_EVAL_CHECKPOINT, TEMPLATE_TRAIN_EVAL, TEMPLATE_TRAIN_ONLY
from .models import StarterTemplate
from .templates import (
    EVAL_CHECKPOINT_TEMPLATE,
    TRAIN_EVAL_TEMPLATE,
    TRAIN_ONLY_TEMPLATE,
)


TEMPLATES: dict[str, StarterTemplate] = {
    TEMPLATE_TRAIN_EVAL: TRAIN_EVAL_TEMPLATE,
    TEMPLATE_TRAIN_ONLY: TRAIN_ONLY_TEMPLATE,
    TEMPLATE_EVAL_CHECKPOINT: EVAL_CHECKPOINT_TEMPLATE,
}


def get_template(name: str) -> StarterTemplate:
    try:
        return TEMPLATES[name]
    except KeyError as exc:
        raise UsageError(f"Unknown starter template: {name}") from exc


def template_choices() -> tuple[str, ...]:
    return tuple(TEMPLATES)


def template_descriptions() -> tuple[tuple[str, str], ...]:
    return tuple((name, template.description) for name, template in TEMPLATES.items())
