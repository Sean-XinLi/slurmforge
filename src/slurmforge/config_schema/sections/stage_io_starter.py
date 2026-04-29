from __future__ import annotations

from typing import Final

from ...config_contract.starter_io import (
    ACCURACY_FILE,
    ACCURACY_JSON_PATH,
    ACCURACY_OUTPUT_NAME,
    CHECKPOINT_GLOB,
    CHECKPOINT_INPUT_NAME,
    CHECKPOINT_OUTPUT_NAME,
)
from ...config_contract.workflows import EVAL_TEMPLATES, STAGE_EVAL, STAGE_TRAIN
from ...config_contract.workflows import TRAIN_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path=f"stages.{STAGE_TRAIN}.outputs.{CHECKPOINT_OUTPUT_NAME}",
        title="Train checkpoint output",
        short_help="Declared checkpoint contract produced by the train stage.",
        when_to_change="Change the discovery glob if your training code writes checkpoints somewhere else.",
        section="Stage IO",
        level="common",
        templates=TRAIN_TEMPLATES,
        default_display=CHECKPOINT_GLOB,
        first_edit=True,
    ),
    ConfigField(
        path=f"stages.{STAGE_EVAL}.inputs.{CHECKPOINT_INPUT_NAME}",
        title="Eval checkpoint input",
        short_help="Checkpoint input consumed by the eval stage.",
        when_to_change="For train-eval keep upstream_output; for eval-checkpoint replace the sample external path.",
        section="Stage IO",
        level="common",
        templates=EVAL_TEMPLATES,
        default_display="template-specific",
        first_edit=True,
    ),
    ConfigField(
        path=f"stages.{STAGE_EVAL}.outputs.{ACCURACY_OUTPUT_NAME}",
        title="Eval metric output",
        short_help=f"Declared {ACCURACY_OUTPUT_NAME} metric read from {ACCURACY_FILE}.",
        when_to_change="Change this if evaluation writes a different metric file or JSON path.",
        section="Stage IO",
        level="common",
        templates=EVAL_TEMPLATES,
        default_display=f"{ACCURACY_FILE} {ACCURACY_JSON_PATH}",
        first_edit=True,
    ),
)
