from __future__ import annotations

from typing import Any

from ...config_comments import option_comment
from ...defaults import TEMPLATE_EVAL_CHECKPOINT
from ..scalar import scalar


def render_inputs(
    lines: list[str], template_name: str, inputs: dict[str, Any]
) -> None:
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
            and source["kind"] == "external_path"
        ):
            lines.extend(
                [
                    "          # This starter writes checkpoint.pt as a sample input.",
                    "          # Replace it with your real checkpoint path before submit.",
                ]
            )
        lines.append(f"          kind: {scalar(source['kind'])}")
        if source["kind"] == "upstream_output":
            lines.extend(
                [
                    "          # Consume an output produced by a previous stage.",
                    f"          stage: {scalar(source['stage'])}",
                    f"          output: {scalar(source['output'])}",
                ]
            )
        if source["kind"] == "external_path":
            lines.append(f"          path: {scalar(source['path'])}")
        lines.extend(
            [
                option_comment("stages.*.inputs.*.expects", indent=8),
                f"        expects: {scalar(input_spec['expects'])}",
                f"        required: {scalar(input_spec['required'])}",
                "        inject:",
                "          # flag passes --<flag>; env sets an environment variable.",
                f"          flag: {scalar(inject['flag'])}",
                f"          env: {scalar(inject['env'])}",
                option_comment("stages.*.inputs.*.inject.mode", indent=10),
                f"          mode: {scalar(inject['mode'])}",
            ]
        )
