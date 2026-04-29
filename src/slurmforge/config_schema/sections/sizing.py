from __future__ import annotations

from typing import Final

from ...config_contract.defaults import (
    DEFAULT_GPU_SIZING_ROUND_TO,
    DEFAULT_GPU_SIZING_SAFETY_FACTOR,
)
from ...config_contract.workflows import ALL_STARTER_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="sizing.gpu.defaults",
        title="GPU sizing defaults",
        short_help="Default safety and rounding policy for automatic GPU sizing.",
        when_to_change="Define this when stages use gpu_sizing and should share conservative sizing behavior.",
        section="Sizing",
        level="advanced",
        value_type="mapping",
        templates=ALL_STARTER_TEMPLATES,
        default_display=(
            f"safety_factor={DEFAULT_GPU_SIZING_SAFETY_FACTOR}, "
            f"round_to={DEFAULT_GPU_SIZING_ROUND_TO}"
        ),
    ),
    ConfigField(
        path="sizing.gpu.defaults.safety_factor",
        title="GPU sizing safety factor",
        short_help="Multiplier applied to estimated GPU memory before converting to GPU count.",
        when_to_change="Increase this when estimates are optimistic or workloads have variable peak memory.",
        section="Sizing",
        level="advanced",
        value_type="float",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_GPU_SIZING_SAFETY_FACTOR,
    ),
    ConfigField(
        path="sizing.gpu.defaults.round_to",
        title="GPU sizing round-to",
        short_help="GPU count granularity used after automatic sizing.",
        when_to_change="Use this to round GPU counts to scheduler or launcher-friendly sizes.",
        section="Sizing",
        level="advanced",
        value_type="integer",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_GPU_SIZING_ROUND_TO,
    ),
)
