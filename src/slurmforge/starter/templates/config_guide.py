from __future__ import annotations

from ...config_schema import render_template_config_guide


def render_starter_config_guide(template: str) -> str:
    return render_template_config_guide(template)
