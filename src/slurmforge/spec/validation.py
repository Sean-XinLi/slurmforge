from __future__ import annotations

from pathlib import Path
from typing import Any

from ..schema import inject_mode_matches_expectation, resolved_kind_for_output_kind
from ..errors import ConfigContractError
from .models import ExperimentSpec, StageSpec
from .queries import normalize_matrix_path


def _resolve_workdir(spec: ExperimentSpec, stage: StageSpec) -> Path:
    workdir = Path(stage.entry.workdir)
    return workdir if workdir.is_absolute() else spec.project_root / workdir


def _resolve_script(spec: ExperimentSpec, stage: StageSpec) -> Path:
    script = Path(str(stage.entry.script))
    if script.is_absolute():
        return script
    return _resolve_workdir(spec, stage) / script


def _path_exists_or_allowed_for_args(raw: dict[str, Any], path: str) -> bool:
    normalized = normalize_matrix_path(raw, path)
    parts = normalized.split(".")
    if len(parts) >= 5 and parts[:2] == ["stages", parts[1]] and parts[2:4] == ["entry", "args"]:
        return parts[1] in {"train", "eval"}
    cursor: Any = raw
    for part in parts:
        if not isinstance(cursor, dict) or part not in cursor:
            return False
        cursor = cursor[part]
    return True


def _explicit_int(raw: Any, *, field: str) -> int | None:
    if raw in (None, "", "auto"):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigContractError(f"`{field}` must be an integer or auto") from exc


def _require_port(raw: Any, *, field: str) -> int:
    value = _explicit_int(raw, field=field)
    if value is None:
        value = 29500
    if value < 1 or value > 65535:
        raise ConfigContractError(f"`{field}` must be between 1 and 65535")
    return value


def _validate_stage_contract(spec: ExperimentSpec, stage: StageSpec, *, check_paths: bool) -> None:
    if stage.name not in {"train", "eval"}:
        raise ConfigContractError("Stage-batch v1 only supports train and eval")
    if stage.kind != stage.name:
        raise ConfigContractError(f"`stages.{stage.name}.kind` must match the stage key")
    if stage.name == "train" and stage.depends_on:
        raise ConfigContractError("train must not depend on any stage")
    if stage.name == "eval" and stage.depends_on not in {(), ("train",)}:
        raise ConfigContractError("eval may only depend on train")
    if stage.resources.nodes < 1:
        raise ConfigContractError(f"`stages.{stage.name}.resources.nodes` must be >= 1")
    if stage.resources.gpus_per_node < 0:
        raise ConfigContractError(f"`stages.{stage.name}.resources.gpus_per_node` must be >= 0")
    if stage.resources.cpus_per_task < 1:
        raise ConfigContractError(f"`stages.{stage.name}.resources.cpus_per_task` must be >= 1")
    if stage.launcher.type not in {"single", "python", "torchrun", "srun", "mpirun", "command"}:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type` must be single, python, torchrun, srun, mpirun, or command"
        )
    if stage.launcher.type == "torchrun":
        if stage.entry.type != "python_script":
            raise ConfigContractError(f"`stages.{stage.name}.launcher.type=torchrun` requires a python_script entry")
        mode = str(stage.launcher.options.get("mode") or ("multi_node" if stage.resources.nodes > 1 else "single_node"))
        if mode not in {"single_node", "multi_node"}:
            raise ConfigContractError(f"`stages.{stage.name}.launcher.mode` must be single_node or multi_node")
        if mode == "single_node" and stage.resources.nodes != 1:
            raise ConfigContractError(
                f"`stages.{stage.name}.launcher.mode=single_node` requires resources.nodes == 1"
            )
        if mode == "multi_node" and stage.resources.nodes < 2:
            raise ConfigContractError(f"`stages.{stage.name}.launcher.mode=multi_node` requires resources.nodes >= 2")
        nnodes = _explicit_int(
            stage.launcher.options.get("nnodes", stage.launcher.options.get("nodes", "auto")),
            field=f"stages.{stage.name}.launcher.nnodes",
        )
        if nnodes is not None and nnodes != stage.resources.nodes:
            raise ConfigContractError(
                f"`stages.{stage.name}.launcher.nnodes` must equal resources.nodes "
                f"({stage.resources.nodes})"
            )
        nproc_per_node = _explicit_int(
            stage.launcher.options.get(
                "nproc_per_node",
                stage.launcher.options.get("processes_per_node", "auto"),
            ),
            field=f"stages.{stage.name}.launcher.nproc_per_node",
        )
        if nproc_per_node is not None:
            if nproc_per_node < 1:
                raise ConfigContractError(f"`stages.{stage.name}.launcher.nproc_per_node` must be >= 1")
            if stage.resources.gpus_per_node > 0 and nproc_per_node > stage.resources.gpus_per_node:
                raise ConfigContractError(
                    f"`stages.{stage.name}.launcher.nproc_per_node` cannot exceed "
                    f"resources.gpus_per_node ({stage.resources.gpus_per_node})"
                )
        rendezvous = stage.launcher.options.get("rendezvous") or {}
        if not isinstance(rendezvous, dict):
            raise ConfigContractError(f"`stages.{stage.name}.launcher.rendezvous` must be a mapping")
        _require_port(
            rendezvous.get("port", stage.launcher.options.get("master_port", 29500)),
            field=f"stages.{stage.name}.launcher.rendezvous.port",
        )
    if stage.entry.type == "command" and stage.launcher.type not in {"single", "command", "srun", "mpirun"}:
        raise ConfigContractError(
            f"`stages.{stage.name}.launcher.type={stage.launcher.type}` cannot wrap command entries"
        )
    if stage.runtime not in spec.runtime.user:
        raise ConfigContractError(f"`stages.{stage.name}.runtime` references unknown runtime `{stage.runtime}`")
    for arg in stage.resources.extra_sbatch_args:
        if "\n" in arg:
            raise ConfigContractError(f"`stages.{stage.name}.resources.extra_sbatch_args` cannot contain newlines")
    if check_paths:
        workdir = _resolve_workdir(spec, stage)
        if not workdir.exists() or not workdir.is_dir():
            raise ConfigContractError(f"`stages.{stage.name}.entry.workdir` does not exist: {workdir}")
        if stage.entry.type == "python_script":
            script = _resolve_script(spec, stage)
            if not script.exists() or not script.is_file():
                raise ConfigContractError(f"`stages.{stage.name}.entry.script` does not exist: {script}")
    if stage.depends_on and not any(input_spec.required for input_spec in stage.inputs.values()):
        raise ConfigContractError(f"`stages.{stage.name}` depends on upstream stages but declares no required inputs")
    for input_name, input_spec in stage.inputs.items():
        if input_spec.inject.mode not in {"path", "value", "json"}:
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.inject.mode` must be path, value, or json"
            )
        if not inject_mode_matches_expectation(input_spec.inject.mode, input_spec.expects):
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.inject.mode` is not compatible with "
                f"expects={input_spec.expects}"
            )
        if input_spec.source.kind == "upstream_output":
            upstream_stage = input_spec.source.stage
            output_name = input_spec.source.output
            if not upstream_stage:
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.source.stage` is required"
                )
            if upstream_stage not in spec.enabled_stages:
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.source.stage` references unknown stage `{upstream_stage}`"
                )
            if not output_name:
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.source.output` is required"
                )
            upstream_outputs = spec.enabled_stages[upstream_stage].outputs.outputs
            if output_name not in upstream_outputs:
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.source.output` references missing output "
                    f"`{upstream_stage}.{output_name}`"
                )
            output_kind = upstream_outputs[output_name].kind
            expected_kind = resolved_kind_for_output_kind(output_kind)
            if input_spec.expects != expected_kind:
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.expects={input_spec.expects}` is not compatible "
                    f"with output `{upstream_stage}.{output_name}` kind={output_kind}; expected {expected_kind}"
                )
            if stage.depends_on and upstream_stage not in stage.depends_on:
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.source.stage` must reference one of "
                    f"the stage dependencies: {', '.join(stage.depends_on)}"
                )
        elif input_spec.source.kind == "external_path":
            if input_spec.expects == "value":
                raise ConfigContractError(
                    f"`stages.{stage.name}.inputs.{input_name}.expects=value` cannot use external_path"
                )
            if check_paths:
                source_path = Path(input_spec.source.path).expanduser()
                source_path = source_path if source_path.is_absolute() else spec.project_root / source_path
                if not source_path.exists():
                    raise ConfigContractError(
                        f"`stages.{stage.name}.inputs.{input_name}.source.path` does not exist: {source_path}"
                    )
        else:
            raise ConfigContractError(
                f"`stages.{stage.name}.inputs.{input_name}.source.kind` is not supported: {input_spec.source.kind}"
            )
    for output_name, output in stage.outputs.outputs.items():
        output_path = f"stages.{stage.name}.outputs.{output_name}"
        if output.kind in {"file", "files"}:
            if not output.discover.globs:
                raise ConfigContractError(f"`{output_path}.discover.globs` is required")
            for pattern in output.discover.globs:
                if "\n" in pattern:
                    raise ConfigContractError(f"`{output_path}.discover.globs` cannot contain newlines")
        elif output.kind == "metric":
            if not output.file:
                raise ConfigContractError(f"`{output_path}.file` is required")
            if "\n" in output.file:
                raise ConfigContractError(f"`{output_path}.file` cannot contain newlines")
            if not output.json_path.startswith("$"):
                raise ConfigContractError(f"`{output_path}.json_path` must start with `$`")
        elif output.kind == "manifest":
            if not output.file:
                raise ConfigContractError(f"`{output_path}.file` is required")
            if "\n" in output.file:
                raise ConfigContractError(f"`{output_path}.file` cannot contain newlines")


def validate_experiment_spec(spec: ExperimentSpec, *, check_paths: bool = True) -> None:
    unknown_stages = sorted(set(spec.stages) - {"train", "eval"})
    if unknown_stages:
        raise ConfigContractError(f"Unsupported stage keys: {', '.join(unknown_stages)}")
    if "eval" in spec.enabled_stages and spec.enabled_stages["eval"].depends_on and "train" not in spec.enabled_stages:
        raise ConfigContractError("enabled eval depends on train, but train is not enabled")
    for path, _values in spec.matrix_axes:
        if not _path_exists_or_allowed_for_args(spec.raw, path):
            raise ConfigContractError(f"`matrix.axes.{path}` does not target a known config path")
    if spec.dispatch.max_available_gpus < 0:
        raise ConfigContractError("`dispatch.max_available_gpus` must be >= 0")
    if spec.orchestration.controller_cpus < 1:
        raise ConfigContractError("`orchestration.controller_cpus` must be >= 1")
    if not spec.runtime.executor.python.bin:
        raise ConfigContractError("`runtime.executor.python.bin` must not be empty")
    if not spec.runtime.executor.python.min_version:
        raise ConfigContractError("`runtime.executor.python.min_version` must not be empty")
    if not spec.runtime.executor.executor_module:
        raise ConfigContractError("`runtime.executor.module` must not be empty")
    if spec.runtime.executor.bootstrap_scope != "sbatch":
        raise ConfigContractError("`runtime.executor.bootstrap.scope` must be sbatch")
    if "default" not in spec.runtime.user:
        raise ConfigContractError("`runtime.user.default` is required")
    for runtime_name, runtime in spec.runtime.user.items():
        if not runtime.python.bin:
            raise ConfigContractError(f"`runtime.user.{runtime_name}.python.bin` must not be empty")
        if not runtime.python.min_version:
            raise ConfigContractError(f"`runtime.user.{runtime_name}.python.min_version` must not be empty")
    for index, step in enumerate(spec.runtime.executor.bootstrap_steps):
        step_type = str(step.get("type") or "")
        if step_type == "module_load":
            if step.get("name") in (None, ""):
                raise ConfigContractError(f"`runtime.executor.bootstrap.steps[{index}].name` is required")
            if "\n" in str(step.get("name")):
                raise ConfigContractError(f"`runtime.executor.bootstrap.steps[{index}].name` cannot contain newlines")
        elif step_type == "source":
            if step.get("path") in (None, ""):
                raise ConfigContractError(f"`runtime.executor.bootstrap.steps[{index}].path` is required")
            if "\n" in str(step.get("path")):
                raise ConfigContractError(f"`runtime.executor.bootstrap.steps[{index}].path` cannot contain newlines")
            args = step.get("args") or ()
            if not isinstance(args, list):
                raise ConfigContractError(f"`runtime.executor.bootstrap.steps[{index}].args` must be a list")
            for arg in args:
                if "\n" in str(arg):
                    raise ConfigContractError(f"`runtime.executor.bootstrap.steps[{index}].args` cannot contain newlines")
        else:
            raise ConfigContractError(
                f"`runtime.executor.bootstrap.steps[{index}].type` must be module_load or source"
            )
    if spec.artifact_store.strategy not in {"copy", "hardlink", "symlink", "register_only"}:
        raise ConfigContractError("`artifact_store.strategy` must be copy, hardlink, symlink, or register_only")
    if spec.artifact_store.fallback_strategy is not None and spec.artifact_store.fallback_strategy not in {
        "copy",
        "hardlink",
        "symlink",
        "register_only",
    }:
        raise ConfigContractError(
            "`artifact_store.fallback_strategy` must be copy, hardlink, symlink, or register_only"
        )
    for stage in spec.enabled_stages.values():
        _validate_stage_contract(spec, stage, check_paths=check_paths)
