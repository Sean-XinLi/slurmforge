from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigContractError
from ..io import content_digest
from ..overrides import deep_set, parse_override
from .output_contract import parse_stage_output_contract
from .models import (
    ArtifactStoreSpec,
    DispatchSpec,
    EntrySpec,
    ExecutorRuntimeSpec,
    ExperimentSpec,
    InputSource,
    InputInjection,
    LauncherSpec,
    OrchestrationSpec,
    PythonRuntimeSpec,
    ResourceSpec,
    RuntimeSpec,
    StageInputSpec,
    StageSpec,
    StorageSpec,
    UserRuntimeSpec,
)


_ALLOWED_TOP_LEVEL_KEYS = {
    "project",
    "experiment",
    "storage",
    "matrix",
    "runtime",
    "artifact_store",
    "stages",
    "dispatch",
    "orchestration",
}


def load_raw_config(config_path: Path, cli_overrides: tuple[str, ...] = ()) -> dict[str, Any]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigContractError(f"Config must be a YAML mapping: {config_path}")
    raw = copy.deepcopy(raw)
    for override in cli_overrides:
        key, value = parse_override(override)
        deep_set(raw, key, value)
    return raw


def load_experiment_spec(
    config_path: Path,
    *,
    cli_overrides: tuple[str, ...] = (),
    project_root: Path | None = None,
) -> ExperimentSpec:
    resolved = config_path.resolve()
    root = project_root.resolve() if project_root is not None else resolved.parent
    raw = load_raw_config(resolved, cli_overrides)
    spec = parse_experiment_spec(raw, config_path=resolved, project_root=root)
    from .validation import validate_experiment_spec

    validate_experiment_spec(spec)
    return spec


def _require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigContractError(f"`{name}` must be a mapping")
    return value


def _optional_mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return _require_mapping(value, name)


def _parse_entry(raw: Any, *, name: str) -> EntrySpec:
    data = _require_mapping(raw, f"stages.{name}.entry")
    entry_type = str(data.get("type") or ("command" if "command" in data else "python_script"))
    if entry_type not in {"python_script", "command"}:
        raise ConfigContractError(f"`stages.{name}.entry.type` must be `python_script` or `command`")
    script = data.get("script")
    command = data.get("command")
    if entry_type == "python_script" and not script:
        raise ConfigContractError(f"`stages.{name}.entry.script` is required for python_script stages")
    if entry_type == "command" and command in (None, "", []):
        raise ConfigContractError(f"`stages.{name}.entry.command` is required for command stages")
    args = _optional_mapping(data.get("args"), f"stages.{name}.entry.args")
    return EntrySpec(
        type=entry_type,
        script=None if script in (None, "") else str(script),
        command=command,
        workdir=str(data.get("workdir") or "."),
        args=copy.deepcopy(args),
    )


def _parse_resources(raw: Any, *, name: str) -> ResourceSpec:
    data = _optional_mapping(raw, f"stages.{name}.resources")
    extra = data.get("extra_sbatch_args") or ()
    if isinstance(extra, str):
        extra_args = (extra,)
    else:
        extra_args = tuple(str(item) for item in extra)
    return ResourceSpec(
        partition=None if data.get("partition") in (None, "") else str(data.get("partition")),
        account=None if data.get("account") in (None, "") else str(data.get("account")),
        qos=None if data.get("qos") in (None, "") else str(data.get("qos")),
        time_limit=None if data.get("time_limit") in (None, "") else str(data.get("time_limit")),
        nodes=int(data.get("nodes", 1)),
        gpus_per_node=int(data.get("gpus_per_node", 0)),
        cpus_per_task=int(data.get("cpus_per_task", 1)),
        mem=None if data.get("mem") in (None, "") else str(data.get("mem")),
        constraint=None if data.get("constraint") in (None, "") else str(data.get("constraint")),
        extra_sbatch_args=extra_args,
    )


def _parse_python_runtime(raw: Any, *, name: str) -> PythonRuntimeSpec:
    data = _require_mapping(raw, name)
    if data.get("bin") in (None, ""):
        raise ConfigContractError(f"`{name}.bin` is required")
    return PythonRuntimeSpec(
        bin=str(data["bin"]),
        min_version=str(data.get("min_version") or "3.10"),
    )


def _parse_executor_runtime(raw: Any) -> ExecutorRuntimeSpec:
    data = _require_mapping(raw, "runtime.executor")
    bootstrap = copy.deepcopy(_optional_mapping(data.get("bootstrap"), "runtime.executor.bootstrap"))
    steps_raw = bootstrap.get("steps")
    if steps_raw is None:
        steps_raw = []
    if not isinstance(steps_raw, list):
        raise ConfigContractError("`runtime.executor.bootstrap.steps` must be a list")
    steps = tuple(copy.deepcopy(_require_mapping(step, "runtime.executor.bootstrap.steps[]")) for step in steps_raw)
    return ExecutorRuntimeSpec(
        python=_parse_python_runtime(data.get("python"), name="runtime.executor.python"),
        executor_module=str(data.get("module") or "slurmforge.executor.stage"),
        bootstrap_scope=str(bootstrap.get("scope") or "sbatch"),
        bootstrap_steps=steps,
        env=copy.deepcopy(_optional_mapping(data.get("env"), "runtime.executor.env")),
    )


def _parse_user_runtime(raw: Any, *, name: str) -> UserRuntimeSpec:
    data = _require_mapping(raw, name)
    return UserRuntimeSpec(
        python=_parse_python_runtime(data.get("python"), name=f"{name}.python"),
        env=copy.deepcopy(_optional_mapping(data.get("env"), f"{name}.env")),
    )


def _parse_runtime(raw: Any) -> RuntimeSpec:
    data = _require_mapping(raw, "runtime")
    user_raw = _require_mapping(data.get("user"), "runtime.user")
    user = {
        str(name): _parse_user_runtime(value, name=f"runtime.user.{name}")
        for name, value in sorted(user_raw.items())
    }
    if "default" not in user:
        raise ConfigContractError("`runtime.user.default` is required")
    return RuntimeSpec(
        executor=_parse_executor_runtime(data.get("executor")),
        user=user,
    )


def _parse_artifact_store(raw: Any) -> ArtifactStoreSpec:
    data = _optional_mapping(raw, "artifact_store")
    fallback = data.get("fallback_strategy")
    return ArtifactStoreSpec(
        strategy=str(data.get("strategy") or "copy"),
        fallback_strategy=None if fallback in (None, "") else str(fallback),
        verify_digest=bool(data.get("verify_digest", True)),
        fail_on_verify_error=bool(data.get("fail_on_verify_error", True)),
    )


def _parse_launcher(raw: Any, *, name: str) -> LauncherSpec:
    data = _optional_mapping(raw, f"stages.{name}.launcher")
    launcher_type = str(data.get("type") or "single")
    options = copy.deepcopy(data)
    options.pop("type", None)
    return LauncherSpec(type=launcher_type, options=options)


def _parse_inputs(raw: Any, *, stage_name: str) -> dict[str, StageInputSpec]:
    data = _optional_mapping(raw, f"stages.{stage_name}.inputs")
    parsed: dict[str, StageInputSpec] = {}
    for input_name, input_raw in data.items():
        input_data = _optional_mapping(input_raw, f"stages.{stage_name}.inputs.{input_name}")
        inject_data = _optional_mapping(input_data.get("inject"), f"stages.{stage_name}.inputs.{input_name}.inject")
        source_data = _require_mapping(input_data.get("source"), f"stages.{stage_name}.inputs.{input_name}.source")
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


def _parse_stage(name: str, raw: Any) -> StageSpec:
    data = _require_mapping(raw, f"stages.{name}")
    if name not in {"train", "eval"}:
        raise ConfigContractError("Stage-batch v1 only supports `stages.train` and `stages.eval`")
    kind = name
    if "kind" in data and data.get("kind") not in (None, "", name):
        raise ConfigContractError(f"`stages.{name}.kind` must match the stage key `{name}`")
    depends_raw = data.get("depends_on") or ()
    if isinstance(depends_raw, str):
        depends = (depends_raw,)
    else:
        depends = tuple(str(item) for item in depends_raw)
    stage = StageSpec(
        name=name,
        kind=kind,
        enabled=bool(data.get("enabled", True)),
        depends_on=depends,
        entry=_parse_entry(data.get("entry"), name=name),
        resources=_parse_resources(data.get("resources"), name=name),
        launcher=_parse_launcher(data.get("launcher"), name=name),
        runtime=str(data.get("runtime") or "default"),
        inputs=_parse_inputs(data.get("inputs"), stage_name=name),
        outputs=parse_stage_output_contract(data.get("outputs"), stage_name=name),
    )
    if stage.name == "train" and stage.depends_on:
        raise ConfigContractError("`stages.train.depends_on` is not allowed")
    if stage.name == "eval" and stage.depends_on not in {(), ("train",)}:
        raise ConfigContractError("`stages.eval.depends_on` must be omitted or exactly `train`")
    return stage


def _parse_matrix_axes(raw: dict[str, Any]) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    matrix = _optional_mapping(raw.get("matrix"), "matrix")
    axes = _optional_mapping(matrix.get("axes"), "matrix.axes")
    parsed: list[tuple[str, tuple[Any, ...]]] = []
    for key in sorted(axes):
        values = axes[key]
        if not isinstance(values, list) or not values:
            raise ConfigContractError(f"`matrix.axes.{key}` must be a non-empty list")
        parsed.append((str(key), tuple(copy.deepcopy(values))))
    return tuple(parsed)


def _parse_dispatch(raw: Any) -> DispatchSpec:
    data = _optional_mapping(raw, "dispatch")
    policy = str(data.get("overflow_policy") or "serialize_groups")
    if policy not in {"serialize_groups", "error", "best_effort"}:
        raise ConfigContractError("`dispatch.overflow_policy` must be serialize_groups, error, or best_effort")
    return DispatchSpec(
        max_available_gpus=int(data.get("max_available_gpus", 0) or 0),
        overflow_policy=policy,
    )


def _parse_orchestration(raw: Any) -> OrchestrationSpec:
    data = _optional_mapping(raw, "orchestration")
    return OrchestrationSpec(
        controller_partition=None if data.get("controller_partition") in (None, "") else str(data.get("controller_partition")),
        controller_cpus=int(data.get("controller_cpus", 1) or 1),
        controller_mem=None if data.get("controller_mem") in (None, "") else str(data.get("controller_mem")),
        controller_time_limit=None
        if data.get("controller_time_limit") in (None, "")
        else str(data.get("controller_time_limit")),
    )


def parse_experiment_spec(
    raw: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path,
    forced_digest: str | None = None,
) -> ExperimentSpec:
    if "stages" not in raw:
        raise ConfigContractError("Configs must use top-level `stages`")
    if "eval" in raw or "run" in raw or "model" in raw:
        raise ConfigContractError("Top-level `model`, `run`, and `eval` are legacy fields; use `stages.<name>`")
    unknown_top_level = sorted(set(raw) - _ALLOWED_TOP_LEVEL_KEYS)
    if unknown_top_level:
        joined = ", ".join(str(item) for item in unknown_top_level)
        raise ConfigContractError(f"Unsupported top-level keys: {joined}")
    project = raw.get("project")
    experiment = raw.get("experiment")
    if project in (None, ""):
        raise ConfigContractError("`project` is required")
    if experiment in (None, ""):
        raise ConfigContractError("`experiment` is required")
    storage = _require_mapping(raw.get("storage"), "storage")
    storage_root = storage.get("root")
    if storage_root in (None, ""):
        raise ConfigContractError("`storage.root` is required")
    stages_raw = _require_mapping(raw.get("stages"), "stages")
    unknown_stages = sorted(set(stages_raw) - {"train", "eval"})
    if unknown_stages:
        joined = ", ".join(str(item) for item in unknown_stages)
        raise ConfigContractError(f"Unsupported stage keys: {joined}. Stage-batch v1 only supports train and eval")
    stages = {str(name): _parse_stage(str(name), stage_raw) for name, stage_raw in stages_raw.items()}
    enabled = {name: stage for name, stage in stages.items() if stage.enabled}
    if not enabled:
        raise ConfigContractError("At least one stage must be enabled")
    if "eval" in enabled and "train" not in enabled and enabled["eval"].depends_on:
        raise ConfigContractError("`stages.eval.depends_on` requires enabled `stages.train`")
    for name, stage in enabled.items():
        for dep in stage.depends_on:
            if dep not in enabled:
                raise ConfigContractError(f"`stages.{name}.depends_on` references disabled or unknown stage `{dep}`")
    spec = ExperimentSpec(
        project=str(project),
        experiment=str(experiment),
        storage=StorageSpec(root=str(storage_root)),
        matrix_axes=_parse_matrix_axes(raw),
        runtime=_parse_runtime(raw.get("runtime")),
        artifact_store=_parse_artifact_store(raw.get("artifact_store")),
        stages=stages,
        dispatch=_parse_dispatch(raw.get("dispatch")),
        orchestration=_parse_orchestration(raw.get("orchestration")),
        project_root=project_root.resolve(),
        config_path=config_path.resolve(),
        spec_snapshot_digest=forced_digest or content_digest(raw),
        raw=copy.deepcopy(raw),
    )
    spec.stage_order()
    return spec
