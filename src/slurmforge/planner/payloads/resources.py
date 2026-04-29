from __future__ import annotations

from ...plans.outputs import ArtifactStorePlan
from ...plans.resources import ControlResourcesPlan, ResourcePlan
from ...sizing.gpu import resolve_stage_gpu_sizing
from ...sizing.models import GpuSizingResolution
from ...spec import ExperimentSpec, StageSpec
from ...spec.sizing import stage_gpu_sizing_inputs


def resource_sizing_payload(
    spec: ExperimentSpec, stage: StageSpec
) -> GpuSizingResolution:
    request, policy, gpu_types, defaults = stage_gpu_sizing_inputs(spec, stage)
    return resolve_stage_gpu_sizing(
        request=request,
        gpu_sizing=policy,
        gpu_types=gpu_types,
        defaults=defaults,
    )


def resource_payload(
    spec: ExperimentSpec, stage: StageSpec, resource_sizing: GpuSizingResolution
) -> ResourcePlan:
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
