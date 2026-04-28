from __future__ import annotations

from typing import Any

from ..config_comments import option_comment
from ..defaults import TEMPLATE_EVAL_CHECKPOINT
from .scalar import scalar


def render_stages(
    lines: list[str], template_name: str, config: dict[str, Any]
) -> None:
    lines.append("stages:")
    stages = config["stages"]
    for index, (stage_name, stage) in enumerate(stages.items()):
        if index:
            lines.append("")
        _render_stage(lines, template_name, stage_name, stage)


def _render_stage(
    lines: list[str],
    template_name: str,
    stage_name: str,
    stage: dict[str, Any],
) -> None:
    lines.extend(
        [
            f"  {stage_name}:",
            "    # Stage role. Starter workflows use train and/or eval.",
            f"    kind: {scalar(stage['kind'])}",
            f"    enabled: {scalar(stage['enabled'])}",
            "    # Must reference keys under environments and runtime.user.",
            f"    environment: {scalar(stage['environment'])}",
            f"    runtime: {scalar(stage['runtime'])}",
        ]
    )
    _render_entry(lines, stage["entry"])
    _render_launcher(lines, stage["launcher"])
    _render_resources(lines, stage["resources"], stage_name=stage_name)
    _render_outputs(lines, stage["outputs"])
    if "depends_on" in stage:
        lines.extend(
            [
                "    # Upstream stages that must complete before this stage is selected.",
                "    depends_on:",
            ]
        )
        for item in stage["depends_on"]:
            lines.append(f"      - {scalar(item)}")
    if "inputs" in stage:
        _render_inputs(lines, template_name, stage["inputs"])


def _render_entry(lines: list[str], entry: dict[str, Any]) -> None:
    lines.extend(
        [
            "    entry:",
            option_comment("stages.*.entry.type", indent=6),
            f"      type: {scalar(entry['type'])}",
            "      # Script path is resolved relative to this YAML file's directory.",
            f"      script: {scalar(entry['script'])}",
            f"      workdir: {scalar(entry['workdir'])}",
            "      # Each key is passed to the script as a CLI flag.",
            "      args:",
        ]
    )
    _render_mapping(lines, entry.get("args") or {}, indent=8)


def _render_launcher(lines: list[str], launcher: dict[str, Any]) -> None:
    lines.extend(
        [
            "    launcher:",
            option_comment("stages.*.launcher.type", indent=6),
            "      # single is best for normal one-process Python scripts.",
            f"      type: {scalar(launcher['type'])}",
        ]
    )


def _render_resources(
    lines: list[str], resources: dict[str, Any], *, stage_name: str
) -> None:
    lines.extend(
        [
            "    resources:",
            f"      # Slurm partition/queue for each {stage_name} task.",
            f"      partition: {scalar(resources['partition'])}",
            f"      nodes: {scalar(resources['nodes'])}",
            '      # Integer GPU count, or "auto" when gpu_sizing is configured.',
            f"      gpus_per_node: {scalar(resources['gpus_per_node'])}",
            f"      cpus_per_task: {scalar(resources['cpus_per_task'])}",
            f"      mem: {scalar(resources['mem'])}",
            f"      time_limit: {scalar(resources['time_limit'])}",
        ]
    )


def _render_outputs(lines: list[str], outputs: dict[str, Any]) -> None:
    lines.append("    outputs:")
    for output_name, output in outputs.items():
        lines.extend(
            [
                f"      {output_name}:",
                option_comment("stages.*.outputs.*.kind", indent=8),
                f"        kind: {scalar(output['kind'])}",
                f"        required: {scalar(output['required'])}",
            ]
        )
        if "discover" in output:
            _render_discovery(lines, output["discover"])
        if output["kind"] in {"metric", "manifest"}:
            lines.extend(
                [
                    "        # JSON file produced by the stage.",
                    f"        file: {scalar(output['file'])}",
                ]
            )
        if output["kind"] == "metric":
            lines.extend(
                [
                    "        # JSONPath for the metric value inside file.",
                    f"        json_path: {scalar(output['json_path'])}",
                ]
            )


def _render_discovery(lines: list[str], discover: dict[str, Any]) -> None:
    lines.extend(
        [
            "        discover:",
            "          # Glob patterns are evaluated under the stage run directory.",
            "          globs:",
        ]
    )
    for pattern in discover.get("globs", ()):
        lines.append(f"            - {scalar(pattern)}")
    if "select" in discover:
        lines.extend(
            [
                option_comment("stages.*.outputs.*.discover.select", indent=10),
                f"          select: {scalar(discover['select'])}",
            ]
        )


def _render_inputs(
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


def _render_mapping(lines: list[str], mapping: dict[str, Any], *, indent: int) -> None:
    if not mapping:
        lines.append(f"{' ' * indent}{{}}")
        return
    prefix = " " * indent
    for key, value in mapping.items():
        lines.append(f"{prefix}{key}: {scalar(value)}")
