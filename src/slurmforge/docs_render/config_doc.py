from __future__ import annotations

from pathlib import Path

from .config_reference import render_global_field_reference
from .markers import replace_between_markers

STARTER_EXAMPLE_START = "<!-- CONFIG_STARTER_EXAMPLE_START -->"
STARTER_EXAMPLE_END = "<!-- CONFIG_STARTER_EXAMPLE_END -->"
ADVANCED_EXAMPLE_START = "<!-- CONFIG_ADVANCED_EXAMPLE_START -->"
ADVANCED_EXAMPLE_END = "<!-- CONFIG_ADVANCED_EXAMPLE_END -->"
REFERENCE_START = "<!-- CONFIG_CONTRACT_REFERENCE_START -->"
REFERENCE_END = "<!-- CONFIG_CONTRACT_REFERENCE_END -->"


def render_config_doc(
    current: str,
    *,
    path: Path,
    starter_example: str,
    advanced_example: str,
) -> str:
    rendered = replace_between_markers(
        current,
        STARTER_EXAMPLE_START,
        STARTER_EXAMPLE_END,
        f"```yaml\n{starter_example.rstrip()}\n```",
        path=path,
    )
    rendered = replace_between_markers(
        rendered,
        ADVANCED_EXAMPLE_START,
        ADVANCED_EXAMPLE_END,
        f"```yaml\n{advanced_example.rstrip()}\n```",
        path=path,
    )
    return replace_between_markers(
        rendered,
        REFERENCE_START,
        REFERENCE_END,
        render_global_field_reference(),
        path=path,
    )
