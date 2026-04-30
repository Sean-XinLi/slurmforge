from __future__ import annotations

import copy
from typing import Any

from ...config_contract.option_sets import (
    ENTRY_COMMAND,
    ENTRY_PYTHON_SCRIPT,
)
from ...config_contract.keys import reject_unknown_config_keys
from ...config_contract.registry import default_for, options_for, options_sentence
from ...errors import ConfigContractError
from ..models import EntrySpec
from ..parse_common import optional_mapping, require_mapping

def parse_entry(raw: Any, *, name: str) -> EntrySpec:
    data = require_mapping(raw, f"stages.{name}.entry")
    reject_unknown_config_keys(data, parent=f"stages.{name}.entry")
    entry_type = str(
        data.get("type")
        or (
            ENTRY_COMMAND
            if ENTRY_COMMAND in data
            else default_for("stages.*.entry.type")
        )
    )
    if entry_type not in options_for("stages.*.entry.type"):
        raise ConfigContractError(
            f"`stages.{name}.entry.type` must be {options_sentence('stages.*.entry.type')}"
        )
    script = data.get("script")
    command = data.get("command")
    if entry_type == ENTRY_PYTHON_SCRIPT and not script:
        raise ConfigContractError(
            f"`stages.{name}.entry.script` is required for python_script stages"
        )
    if entry_type == ENTRY_COMMAND and command in (None, "", []):
        raise ConfigContractError(
            f"`stages.{name}.entry.command` is required for command stages"
        )
    args = optional_mapping(data.get("args"), f"stages.{name}.entry.args")
    return EntrySpec(
        type=entry_type,
        script=None if script in (None, "") else str(script),
        command=command,
        workdir=str(data.get("workdir") or default_for("stages.*.entry.workdir")),
        args=copy.deepcopy(args),
    )
