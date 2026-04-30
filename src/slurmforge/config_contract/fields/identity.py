from __future__ import annotations

from typing import Final

from ..workflows import ALL_STARTER_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="project",
        title="Project name",
        short_help="Names the project namespace used in generated storage paths.",
        when_to_change="Change this before the first real submit so runs land under your project name.",
        section="Identity",
        level="common",
        templates=ALL_STARTER_TEMPLATES,
        default_value="demo",
        required=True,
        first_edit=True,
    ),
    ConfigField(
        path="experiment",
        title="Experiment name",
        short_help="Names this experiment inside the project namespace.",
        when_to_change="Change this for each baseline, ablation, sweep, or production run family.",
        section="Identity",
        level="common",
        templates=ALL_STARTER_TEMPLATES,
        default_value="baseline",
        required=True,
        first_edit=True,
    ),
)
