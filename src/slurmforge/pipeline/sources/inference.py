from __future__ import annotations

from typing import Any


def infer_model_name_from_cfg(run_cfg: dict[str, Any] | None) -> str:
    if not isinstance(run_cfg, dict):
        return "external"
    model_cfg = run_cfg.get("model")
    if isinstance(model_cfg, dict):
        model_name = str(model_cfg.get("name", "") or "").strip()
        if model_name:
            return model_name
    return "external"


def infer_train_mode_from_cfg(run_cfg: dict[str, Any] | None) -> str:
    if not isinstance(run_cfg, dict):
        return "unknown"
    run_section = run_cfg.get("run")
    if not isinstance(run_section, dict):
        return "unknown"
    explicit_mode = str(run_section.get("mode", "") or "").strip().lower()
    if explicit_mode:
        return explicit_mode
    if str(run_section.get("command", "") or "").strip():
        return "command"
    adapter_cfg = run_section.get("adapter")
    if isinstance(adapter_cfg, dict) and str(adapter_cfg.get("script", "") or "").strip():
        return "adapter"
    return "model_cli"
