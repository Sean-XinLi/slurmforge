from __future__ import annotations

from typing import Any

from ....config_contract.options import (
    INPUT_SOURCE_EXTERNAL_PATH,
    INPUT_SOURCE_UPSTREAM_OUTPUT,
)
from ....config_contract.workflows import TEMPLATE_EVAL_CHECKPOINT
from ...config_comments import comment_for, option_comment
from ..scalar import scalar


def render_inputs(lines: list[str], template_name: str, inputs: dict[str, Any]) -> None:
    lines.append("    inputs:")
    for input_name, input_spec in inputs.items():
        source = input_spec["source"]
        inject = input_spec["inject"]
        lines.extend(
            [
                f"      {input_name}:",
                "        source:",
                option_comment("stages.*.inputs.*.source.kind", indent=10),
            ]
        )
        if (
            template_name == TEMPLATE_EVAL_CHECKPOINT
            and source["kind"] == INPUT_SOURCE_EXTERNAL_PATH
        ):
            lines.extend(
                [
                    "          # This starter writes checkpoint.pt as a sample input.",
                    "          # Replace it with your real checkpoint path before submit.",
                ]
            )
        lines.append(f"          kind: {scalar(source['kind'])}")
        if source["kind"] == INPUT_SOURCE_UPSTREAM_OUTPUT:
            lines.extend(
                [
                    comment_for("stages.*.inputs.*.source.stage", indent=10),
                    f"          stage: {scalar(source['stage'])}",
                    comment_for("stages.*.inputs.*.source.output", indent=10),
                    f"          output: {scalar(source['output'])}",
                ]
            )
        if source["kind"] == INPUT_SOURCE_EXTERNAL_PATH:
            lines.append(comment_for("stages.*.inputs.*.source.path", indent=10))
            lines.append(f"          path: {scalar(source['path'])}")
        lines.extend(
            [
                option_comment("stages.*.inputs.*.expects", indent=8),
                f"        expects: {scalar(input_spec['expects'])}",
                f"        required: {scalar(input_spec['required'])}",
                "        inject:",
                comment_for("stages.*.inputs.*.inject.flag", indent=10),
                f"          flag: {scalar(inject['flag'])}",
                comment_for("stages.*.inputs.*.inject.env", indent=10),
                f"          env: {scalar(inject['env'])}",
                option_comment("stages.*.inputs.*.inject.mode", indent=10),
                f"          mode: {scalar(inject['mode'])}",
            ]
        )
