from __future__ import annotations

from typing import Any

import yaml

from .advanced import advanced_example_config


class _IndentedSafeDumper(yaml.SafeDumper):
    def increase_indent(
        self, flow: bool = False, indentless: bool = False
    ) -> Any:
        return super().increase_indent(flow, False)


def render_advanced_example() -> str:
    return yaml.dump(
        advanced_example_config(),
        Dumper=_IndentedSafeDumper,
        sort_keys=False,
        default_flow_style=False,
    )
