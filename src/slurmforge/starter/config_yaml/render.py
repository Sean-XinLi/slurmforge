from __future__ import annotations

from typing import Any

from .sections import (
    render_artifact_store,
    render_dispatch,
    render_environments,
    render_orchestration,
    render_project,
    render_runs,
    render_runtime,
)
from .stages import render_stages


def render_starter_config(template_name: str, config: dict[str, Any]) -> str:
    lines: list[str] = [
        f"# Starter template: {template_name}",
        "# Edit the values below, then run `sforge validate --config <file>`.",
        "# Full field reference: docs/config.md",
        "",
    ]
    render_project(lines, config)
    render_environments(lines, config)
    render_runtime(lines, config)
    render_artifact_store(lines, config)
    render_runs(lines, config)
    render_dispatch(lines, config)
    render_orchestration(lines, config)
    render_stages(lines, template_name, config)
    return "\n".join(lines).rstrip() + "\n"
