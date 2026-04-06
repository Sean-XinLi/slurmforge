from __future__ import annotations

from typing import Any

from ...errors import ConfigContractError
from .constants import RUN_MODES
from .utils import ensure_dict, non_empty_text


def detect_run_mode(run_cfg: dict[str, Any]) -> str:
    command = non_empty_text(run_cfg.get("command"))
    adapter_cfg = ensure_dict(run_cfg.get("adapter"), "run.adapter")
    adapter_script = non_empty_text(adapter_cfg.get("script"))
    explicit_mode = non_empty_text(run_cfg.get("mode"))

    inferred_mode: str
    if command and adapter_script:
        raise ConfigContractError("run.command and run.adapter.script cannot be used together")
    if command:
        inferred_mode = "command"
    elif adapter_script:
        inferred_mode = "adapter"
    else:
        inferred_mode = "model_cli"

    if explicit_mode is None:
        return inferred_mode

    normalized_mode = explicit_mode.lower()
    if normalized_mode not in RUN_MODES:
        raise ConfigContractError(f"run.mode must be one of: {sorted(RUN_MODES)}")
    if normalized_mode != inferred_mode:
        raise ConfigContractError(
            f"run.mode={normalized_mode!r} conflicts with provided run fields; inferred mode is {inferred_mode!r}"
        )
    return normalized_mode
