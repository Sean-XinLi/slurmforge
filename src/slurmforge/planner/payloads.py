from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ..plans import (
    ArtifactStorePlan,
    BeforeStepPlan,
    ControlResourcesPlan,
    EmailNotificationPlan,
    EntryPlan,
    EnvironmentPlan,
    EnvironmentSourcePlan,
    ExecutorRuntimePlan,
    FinalizerPlan,
    LauncherPlan,
    NotificationPlan,
    PythonRuntimePlan,
    RendezvousPlan,
    ResourcePlan,
    RunDefinition,
    RuntimePlan,
    UserRuntimePlan,
)
from ..resolver.defaults import default_stage_input_bindings
from ..contracts import InputBinding
from ..sizing import GpuSizingResolution, resolve_stage_gpu_sizing
from ..spec import ExperimentSpec, StageSpec
from ..spec.sizing import stage_gpu_sizing_inputs


def entry_payload(spec: ExperimentSpec, stage: StageSpec) -> EntryPlan:
    workdir = Path(stage.entry.workdir)
    resolved_workdir = workdir if workdir.is_absolute() else spec.project_root / workdir
    return EntryPlan(
        type=stage.entry.type,
        script=stage.entry.script,
        command=copy.deepcopy(stage.entry.command),
        workdir=str(resolved_workdir.resolve()),
        args=copy.deepcopy(stage.entry.args),
    )


def resource_sizing_payload(spec: ExperimentSpec, stage: StageSpec) -> GpuSizingResolution:
    request, policy, gpu_types, defaults = stage_gpu_sizing_inputs(spec, stage)
    return resolve_stage_gpu_sizing(
        request=request,
        gpu_sizing=policy,
        gpu_types=gpu_types,
        defaults=defaults,
    )


def resource_payload(spec: ExperimentSpec, stage: StageSpec, resource_sizing: GpuSizingResolution) -> ResourcePlan:
    hardware_gpu = spec.hardware.gpu_types.get(stage.resources.gpu_type)
    hardware_slurm = hardware_gpu.slurm if hardware_gpu is not None else {}
    constraint = stage.resources.constraint
    if constraint is None and hardware_slurm.get("constraint") not in (None, ""):
        constraint = str(hardware_slurm["constraint"])
    return ResourcePlan(
        partition=stage.resources.partition,
        account=stage.resources.account,
        qos=stage.resources.qos,
        time_limit=stage.resources.time_limit,
        gpu_type=stage.resources.gpu_type,
        nodes=stage.resources.nodes,
        gpus_per_node=resource_sizing.resolved_gpus_per_node,
        cpus_per_task=stage.resources.cpus_per_task,
        mem=stage.resources.mem,
        constraint=constraint,
        extra_sbatch_args=stage.resources.extra_sbatch_args,
    )


def executor_runtime_payload(spec: ExperimentSpec) -> ExecutorRuntimePlan:
    return ExecutorRuntimePlan(
        python=PythonRuntimePlan(
            bin=spec.runtime.executor.python.bin,
            min_version=spec.runtime.executor.python.min_version,
        ),
        module=spec.runtime.executor.executor_module,
    )


def runtime_payload(spec: ExperimentSpec, stage: StageSpec) -> RuntimePlan:
    user_runtime = spec.runtime.user[stage.runtime]
    return RuntimePlan(
        executor=executor_runtime_payload(spec),
        user=UserRuntimePlan(
            name=stage.runtime,
            python=PythonRuntimePlan(
                bin=user_runtime.python.bin,
                min_version=user_runtime.python.min_version,
            ),
            env=copy.deepcopy(user_runtime.env),
        ),
    )


def environment_payload(spec: ExperimentSpec, name: str) -> EnvironmentPlan:
    if not name:
        return EnvironmentPlan()
    environment = spec.environments[name]
    return EnvironmentPlan(
        name=environment.name,
        modules=environment.modules,
        source=tuple(EnvironmentSourcePlan(path=source.path, args=source.args) for source in environment.source),
        env=copy.deepcopy(environment.env),
    )


def before_payload(stage: StageSpec) -> tuple[BeforeStepPlan, ...]:
    return tuple(BeforeStepPlan(name=step.name, run=step.run) for step in stage.before)


def launcher_payload(stage: StageSpec, resources: ResourcePlan) -> LauncherPlan:
    launcher_type = stage.launcher.type
    options = copy.deepcopy(stage.launcher.options)
    if launcher_type == "torchrun":
        return _torchrun_launcher_payload(stage, resources, options)
    if launcher_type in {"srun", "mpirun"}:
        args = options.get("args") or ()
        return LauncherPlan(type=launcher_type, args=tuple(str(item) for item in args))
    return LauncherPlan(type=launcher_type)


def artifact_store_payload(spec: ExperimentSpec) -> ArtifactStorePlan:
    return ArtifactStorePlan(
        strategy=spec.artifact_store.strategy,
        fallback_strategy=spec.artifact_store.fallback_strategy,
        verify_digest=spec.artifact_store.verify_digest,
        fail_on_verify_error=spec.artifact_store.fail_on_verify_error,
    )


def control_resources_payload(spec: ExperimentSpec) -> ControlResourcesPlan:
    controller = spec.orchestration.controller
    return ControlResourcesPlan(
        partition=controller.partition,
        cpus=controller.cpus,
        mem=controller.mem,
        time_limit=controller.time_limit,
    )


def notification_payload(spec: ExperimentSpec) -> NotificationPlan:
    email = spec.notifications.email
    controller = spec.orchestration.controller
    return NotificationPlan(
        email=EmailNotificationPlan(
            enabled=email.enabled,
            to=email.to,
            events=email.events,
            mode=email.mode,
            from_address=email.from_address,
            sendmail=email.sendmail,
            subject_prefix=email.subject_prefix,
        ),
        finalizer=FinalizerPlan(
            resources=control_resources_payload(spec),
            environment_name=controller.environment,
            environment_plan=environment_payload(spec, controller.environment),
            runtime_plan=RuntimePlan(executor=executor_runtime_payload(spec)),
        ),
    )


def default_bindings(spec: ExperimentSpec, run: RunDefinition, stage: StageSpec) -> tuple[InputBinding, ...]:
    return default_stage_input_bindings(spec, run, stage)


def _torchrun_launcher_payload(stage: StageSpec, resources: ResourcePlan, options: dict[str, Any]) -> LauncherPlan:
    nodes = resources.nodes
    gpus = resources.gpus_per_node
    nproc_default = gpus if gpus > 0 else 1
    mode = str(options.get("mode") or ("multi_node" if nodes > 1 else "single_node"))
    rendezvous = dict(options.get("rendezvous") or {})
    port = rendezvous.get("port", options.get("master_port", 29500))
    master_port = None if options.get("master_port") is None else int(options["master_port"])
    return LauncherPlan(
        type="torchrun",
        mode=mode,
        nnodes=_auto_int(options.get("nnodes") or options.get("nodes") or "auto", nodes),
        nproc_per_node=_auto_int(
            options.get("nproc_per_node") or options.get("processes_per_node") or "auto",
            nproc_default,
        ),
        rendezvous=RendezvousPlan(
            backend=str(rendezvous.get("backend") or "c10d"),
            endpoint=str(rendezvous.get("endpoint") or "auto"),
            port=int(port),
        ),
        srun_args=tuple(str(item) for item in options.get("srun_args") or ()),
        master_port=master_port,
    )


def _auto_int(value: Any, default: int) -> int:
    if value in (None, "", "auto"):
        return int(default)
    return int(value)
