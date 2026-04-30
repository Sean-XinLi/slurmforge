from __future__ import annotations

from ..config_contract.option_sets import ENTRY_PYTHON_SCRIPT
from ..errors import ConfigContractError
from ..sizing.gpu import resolve_stage_gpu_sizing
from .models import ExperimentSpec, StageSpec
from .sizing import stage_gpu_sizing_inputs
from .validation_common import reject_newline, resolve_script, resolve_workdir
from .validation_inputs import validate_stage_inputs_contract
from .validation_launcher import validate_launcher_contract
from .validation_outputs import validate_stage_outputs_contract


def validate_stage_contract(
    spec: ExperimentSpec, stage: StageSpec, *, check_paths: bool
) -> None:
    if stage.resources.nodes < 1:
        raise ConfigContractError(f"`stages.{stage.name}.resources.nodes` must be >= 1")
    request, policy, gpu_types, defaults = stage_gpu_sizing_inputs(spec, stage)
    sizing_resolution = resolve_stage_gpu_sizing(
        request=request,
        gpu_sizing=policy,
        gpu_types=gpu_types,
        defaults=defaults,
    )
    if sizing_resolution.resolved_gpus_per_node < 0:
        raise ConfigContractError(
            f"`stages.{stage.name}.resources.gpus_per_node` must be >= 0"
        )
    if stage.resources.cpus_per_task < 1:
        raise ConfigContractError(
            f"`stages.{stage.name}.resources.cpus_per_task` must be >= 1"
        )
    validate_launcher_contract(stage, sizing_resolution=sizing_resolution)
    if stage.runtime not in spec.runtime.user:
        raise ConfigContractError(
            f"`stages.{stage.name}.runtime` references unknown runtime `{stage.runtime}`"
        )
    if stage.environment and stage.environment not in spec.environments:
        raise ConfigContractError(
            f"`stages.{stage.name}.environment` references unknown environment `{stage.environment}`"
        )
    _validate_before_steps(stage)
    for arg in stage.resources.extra_sbatch_args:
        reject_newline(arg, field=f"stages.{stage.name}.resources.extra_sbatch_args")
    if check_paths:
        _validate_stage_paths(spec, stage)
    validate_stage_inputs_contract(spec, stage, check_paths=check_paths)
    validate_stage_outputs_contract(stage)


def _validate_before_steps(stage: StageSpec) -> None:
    for index, step in enumerate(stage.before):
        field = f"stages.{stage.name}.before[{index}]"
        if not step.run:
            raise ConfigContractError(f"`{field}.run` is required")
        reject_newline(step.run, field=f"{field}.run")
        if step.name:
            reject_newline(step.name, field=f"{field}.name")


def _validate_stage_paths(spec: ExperimentSpec, stage: StageSpec) -> None:
    workdir = resolve_workdir(spec, stage)
    if not workdir.exists() or not workdir.is_dir():
        raise ConfigContractError(
            f"`stages.{stage.name}.entry.workdir` does not exist: {workdir}"
        )
    if stage.entry.type == ENTRY_PYTHON_SCRIPT:
        script = resolve_script(spec, stage)
        if not script.exists() or not script.is_file():
            raise ConfigContractError(
                f"`stages.{stage.name}.entry.script` does not exist: {script}"
            )
