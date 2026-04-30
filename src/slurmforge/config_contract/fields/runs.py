from __future__ import annotations

from typing import Final

from ..default_values import DEFAULT_RUN_TYPE
from ..option_sets import RUN_TYPES
from ..workflows import ALL_STARTER_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="runs.type",
        title="Run expansion type",
        short_help="Controls whether the config plans one run or expands a sweep.",
        when_to_change="Keep single for the starter; switch to grid, cases, or matrix when you need run expansion.",
        section="Runs",
        level="common",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_RUN_TYPE,
        options=RUN_TYPES,
    ),
    ConfigField(
        path="runs.axes",
        title="Grid axes",
        short_help="Top-level grid sweep axes for runs.type=grid.",
        when_to_change="Use this when every combination of selected values should become a run.",
        section="Runs",
        level="intermediate",
        value_type="mapping",
        templates=ALL_STARTER_TEMPLATES,
        default_display="contextual",
        required=None,
    ),
    ConfigField(
        path="runs.cases",
        title="Run cases",
        short_help="Named run variants for runs.type=cases or runs.type=matrix.",
        when_to_change="Use this when runs need stable names or case-specific overrides.",
        section="Runs",
        level="intermediate",
        value_type="list",
        templates=ALL_STARTER_TEMPLATES,
        default_display="contextual",
        required=None,
    ),
    ConfigField(
        path="runs.cases[].name",
        title="Run case name",
        short_help="Stable identifier used in expanded run ids.",
        when_to_change="Set this to a short, filesystem-safe name for each case.",
        section="Runs",
        level="intermediate",
        templates=ALL_STARTER_TEMPLATES,
        default_display="required for cases and matrix",
        required=True,
    ),
    ConfigField(
        path="runs.cases[].set",
        title="Run case overrides",
        short_help="Dot-path overrides applied to a named run case.",
        when_to_change="Use this for hand-authored run variants with explicit settings.",
        section="Runs",
        level="intermediate",
        value_type="mapping",
        templates=ALL_STARTER_TEMPLATES,
        default_display="{}",
        required=False,
    ),
    ConfigField(
        path="runs.cases[].axes",
        title="Run case matrix axes",
        short_help="Case-local grid axes for runs.type=matrix.",
        when_to_change="Use this when each named scenario needs its own sweep dimensions.",
        section="Runs",
        level="advanced",
        value_type="mapping",
        templates=ALL_STARTER_TEMPLATES,
        default_display="contextual",
        required=None,
    ),
)
