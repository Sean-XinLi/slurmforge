from __future__ import annotations

from typing import Any

from ..config_contract.registry import default_for, options_for, options_sentence
from ..errors import ConfigContractError

OUTPUT_SELECTORS = set(options_for("stages.*.outputs.*.discover.select"))


def normalize_output_selector(value: Any) -> str:
    selector = str(value or default_for("stages.*.outputs.*.discover.select"))
    if selector not in OUTPUT_SELECTORS:
        raise ConfigContractError(
            "output discover select must be "
            f"{options_sentence('stages.*.outputs.*.discover.select')}"
        )
    return selector
