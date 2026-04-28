from __future__ import annotations

import copy
from typing import Any

from ..errors import ConfigContractError
from ..contracts import InputInjection, InputSource
from .models import (
    ArtifactStoreSpec,
    BeforeStepSpec,
    EntrySpec,
    LauncherSpec,
    StageGpuSizingSpec,
    StageInputSpec,
    StageSpec,
)
from ..contracts.outputs import parse_stage_output_contract
from .parse_common import optional_mapping, reject_unknown_keys, require_mapping
from .parse_resources import parse_resources


def parse_entry(raw: Any, *, name: str) -> EntrySpec:
    data = require_mapping(raw, f"stages.{name}.entry")
    entry_type = str(data.get("type") or ("command" if "command" in data else "python_script"))
    if entry_type not in {"python_script", "command"}:
        raise ConfigContractError(f"`stages.{name}.entry.type` must be `python_script` or `command`")
    script = data.get("script")
    command = data.get("command")
    if entry_type == "python_script" and not script:
        raise ConfigContractError(f"`stages.{name}.entry.script` is required for python_script stages")
    if entry_type == "command" and command in (None, "", []):
        raise ConfigContractError(f"`stages.{name}.entry.command` is required for command stages")
    args = optional_mapping(data.get("args"), f"stages.{name}.entry.args")
    return EntrySpec(
        type=entry_type,
        script=None if script in (None, "") else str(script),
        command=command,
        workdir=str(data.get("workdir") or "."),
        args=copy.deepcopy(args),
    )


def parse_artifact_store(raw: Any) -> ArtifactStoreSpec:
    data = optional_mapping(raw, "artifact_store")
    fallback = data.get("fallback_strategy")
    return ArtifactStoreSpec(
        strategy=str(data.get("strategy") or "copy"),
        fallback_strategy=None if fallback in (None, "") else str(fallback),
        verify_digest=bool(data.get("verify_digest", True)),
        fail_on_verify_error=bool(data.get("fail_on_verify_error", True)),
    )


def parse_launcher(raw: Any, *, name: str) -> LauncherSpec:
    data = optional_mapping(raw, f"stages.{name}.launcher")
    launcher_type = str(data.get("type") or "single")
    options = copy.deepcopy(data)
    options.pop("type", None)
    return LauncherSpec(type=launcher_type, options=options)


def parse_inputs(raw: Any, *, stage_name: str) -> dict[str, StageInputSpec]:
    data = optional_mapping(raw, f"stages.{stage_name}.inputs")
    parsed: dict[str, StageInputSpec] = {}
    for input_name, input_raw in data.items():
        input_data = optional_mapping(input_raw, f"stages.{stage_name}.inputs.{input_name}")
        inject_data = optional_mapping(input_data.get("inject"), f"stages.{stage_name}.inputs.{input_name}.inject")
        source_data = require_mapping(input_data.get("source"), f"stages.{stage_name}.inputs.{input_name}.source")
        kind = str(source_data.get("kind") or "")
        if kind not in {"upstream_output", "external_path"}:
            raise ConfigContractError(
                f"`stages.{stage_name}.inputs.{input_name}.source.kind` must be upstream_output or external_path"
            )
        if kind == "upstream_output":
            if source_data.get("stage") in (None, ""):
                raise ConfigContractError(f"`stages.{stage_name}.inputs.{input_name}.source.stage` is required")
            if source_data.get("output") in (None, ""):
                raise ConfigContractError(f"`stages.{stage_name}.inputs.{input_name}.source.output` is required")
            source = InputSource(
                kind="upstream_output",
                stage=str(source_data["stage"]),
                output=str(source_data["output"]),
            )
        else:
            if source_data.get("path") in (None, ""):
                raise ConfigContractError(f"`stages.{stage_name}.inputs.{input_name}.source.path` is required")
            source = InputSource(kind="external_path", path=str(source_data["path"]))
        expects = str(input_data.get("expects") or "path")
        if expects not in {"path", "value", "manifest"}:
            raise ConfigContractError(f"`stages.{stage_name}.inputs.{input_name}.expects` must be path, value, or manifest")
        parsed[str(input_name)] = StageInputSpec(
            name=str(input_name),
            source=source,
            expects=expects,
            required=bool(input_data.get("required", False)),
            inject=InputInjection(
                flag=None if inject_data.get("flag") in (None, "") else str(inject_data.get("flag")),
                env=None if inject_data.get("env") in (None, "") else str(inject_data.get("env")),
                mode=str(inject_data.get("mode") or "path"),
            ),
        )
    return parsed


def parse_before(raw: Any, *, stage_name: str) -> tuple[BeforeStepSpec, ...]:
    if raw in (None, ""):
        return ()
    if not isinstance(raw, list):
        raise ConfigContractError(f"`stages.{stage_name}.before` must be a list")
    steps: list[BeforeStepSpec] = []
    for index, item in enumerate(raw):
        name = f"stages.{stage_name}.before[{index}]"
        data = require_mapping(item, name)
        reject_unknown_keys(data, allowed={"name", "run"}, name=name)
        if data.get("run") in (None, ""):
            raise ConfigContractError(f"`{name}.run` is required")
        steps.append(
            BeforeStepSpec(
                name="" if data.get("name") in (None, "") else str(data.get("name")),
                run=str(data["run"]),
            )
        )
    return tuple(steps)


def parse_stage_gpu_sizing(raw: Any, *, stage_name: str) -> StageGpuSizingSpec | None:
    if raw in (None, ""):
        return None
    data = require_mapping(raw, f"stages.{stage_name}.gpu_sizing")
    reject_unknown_keys(
        data,
        allowed={
            "estimator",
            "target_memory_gb",
            "min_gpus_per_job",
            "max_gpus_per_job",
            "safety_factor",
            "round_to",
        },
        name=f"stages.{stage_name}.gpu_sizing",
    )
    if data.get("estimator") in (None, ""):
        raise ConfigContractError(f"`stages.{stage_name}.gpu_sizing.estimator` is required")
    if data.get("target_memory_gb") in (None, ""):
        raise ConfigContractError(f"`stages.{stage_name}.gpu_sizing.target_memory_gb` is required")
    max_gpus = data.get("max_gpus_per_job")
    safety_factor = data.get("safety_factor")
    round_to = data.get("round_to")
    return StageGpuSizingSpec(
        estimator=str(data["estimator"]),
        target_memory_gb=float(data["target_memory_gb"]),
        min_gpus_per_job=int(data.get("min_gpus_per_job", 1) or 1),
        max_gpus_per_job=None if max_gpus in (None, "") else int(max_gpus),
        safety_factor=None if safety_factor in (None, "") else float(safety_factor),
        round_to=None if round_to in (None, "") else int(round_to),
    )


def parse_stage(name: str, raw: Any) -> StageSpec:
    data = require_mapping(raw, f"stages.{name}")
    reject_unknown_keys(
        data,
        allowed={
            "enabled",
            "kind",
            "depends_on",
            "entry",
            "resources",
            "launcher",
            "runtime",
            "environment",
            "gpu_sizing",
            "before",
            "inputs",
            "outputs",
        },
        name=f"stages.{name}",
    )
    kind = str(data.get("kind") or name)
    depends_raw = data.get("depends_on") or ()
    if isinstance(depends_raw, str):
        depends = (depends_raw,)
    else:
        depends = tuple(str(item) for item in depends_raw)
    return StageSpec(
        name=name,
        kind=kind,
        enabled=bool(data.get("enabled", True)),
        depends_on=depends,
        entry=parse_entry(data.get("entry"), name=name),
        resources=parse_resources(data.get("resources"), name=name),
        launcher=parse_launcher(data.get("launcher"), name=name),
        runtime=str(data.get("runtime") or "default"),
        environment="" if data.get("environment") in (None, "") else str(data.get("environment")),
        gpu_sizing=parse_stage_gpu_sizing(data.get("gpu_sizing"), stage_name=name),
        before=parse_before(data.get("before"), stage_name=name),
        inputs=parse_inputs(data.get("inputs"), stage_name=name),
        outputs=parse_stage_output_contract(data.get("outputs"), stage_name=name),
    )
