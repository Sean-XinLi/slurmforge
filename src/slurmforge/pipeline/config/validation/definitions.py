from __future__ import annotations

from dataclasses import dataclass

from ..constants import REPLAY_MODEL_CATALOG_KEY


@dataclass(frozen=True)
class ScalarField:
    pass


@dataclass(frozen=True)
class DynamicMapping:
    pass


@dataclass(frozen=True)
class ObjectSchema:
    fields: dict[str, object]


SCALAR = ScalarField()
DYNAMIC = DynamicMapping()
BATCH_SCOPED_SWEEP_PREFIXES = (
    "project",
    "experiment_name",
    "output",
    "notify",
)

MODEL_SCHEMA = ObjectSchema(
    {
        "name": SCALAR,
        "script": SCALAR,
        "yaml": SCALAR,
        "ddp_supported": SCALAR,
        "ddp_required": SCALAR,
        "estimator_profile": SCALAR,
    }
)
MODEL_REGISTRY_SCHEMA = ObjectSchema(
    {
        "registry_file": SCALAR,
        "extra_models": SCALAR,
    }
)
MODEL_CATALOG_SCHEMA = ObjectSchema(
    {
        "models": SCALAR,
    }
)
DISTRIBUTED_SCHEMA = ObjectSchema(
    {
        "nnodes": SCALAR,
        "nproc_per_node": SCALAR,
        "master_port": SCALAR,
        "port_offset": SCALAR,
        "extra_torchrun_args": SCALAR,
    }
)
LAUNCHER_SCHEMA = ObjectSchema(
    {
        "mode": SCALAR,
        "python_bin": SCALAR,
        "workdir": SCALAR,
        "distributed": DISTRIBUTED_SCHEMA,
        "ddp": DISTRIBUTED_SCHEMA,
    }
)
ADAPTER_SCHEMA = ObjectSchema(
    {
        "script": SCALAR,
        "args": DYNAMIC,
        "launcher": LAUNCHER_SCHEMA,
        "workdir": SCALAR,
        "launch_mode": SCALAR,
        "pass_run_args": SCALAR,
        "run_args_flag": SCALAR,
        "pass_model_overrides": SCALAR,
        "model_overrides_flag": SCALAR,
        "ddp_supported": SCALAR,
        "ddp_required": SCALAR,
    }
)
EXTERNAL_RUNTIME_SCHEMA = ObjectSchema(
    {
        "nnodes": SCALAR,
        "nproc_per_node": SCALAR,
    }
)
RUN_SCHEMA = ObjectSchema(
    {
        "mode": SCALAR,
        "args": DYNAMIC,
        "model_overrides": DYNAMIC,
        "command": SCALAR,
        "command_mode": SCALAR,
        "workdir": SCALAR,
        "resume_from_checkpoint": SCALAR,
        "adapter": ADAPTER_SCHEMA,
        "external_runtime": EXTERNAL_RUNTIME_SCHEMA,
    }
)
CLUSTER_SCHEMA = ObjectSchema(
    {
        "partition": SCALAR,
        "account": SCALAR,
        "qos": SCALAR,
        "time_limit": SCALAR,
        "nodes": SCALAR,
        "gpus_per_node": SCALAR,
        "cpus_per_task": SCALAR,
        "mem": SCALAR,
        "constraint": SCALAR,
        "extra_sbatch_args": SCALAR,
    }
)
ENV_SCHEMA = ObjectSchema(
    {
        "modules": SCALAR,
        "conda_activate": SCALAR,
        "venv_activate": SCALAR,
        "extra_env": DYNAMIC,
    }
)
RESOURCES_SCHEMA = ObjectSchema(
    {
        "auto_gpu": SCALAR,
        "gpu_estimator": SCALAR,
        "target_mem_per_gpu_gb": SCALAR,
        "safety_factor": SCALAR,
        "min_gpus_per_job": SCALAR,
        "max_gpus_per_job": SCALAR,
        "max_available_gpus": SCALAR,
    }
)
ARTIFACTS_SCHEMA = ObjectSchema(
    {
        "checkpoint_globs": SCALAR,
        "eval_csv_globs": SCALAR,
        "eval_image_globs": SCALAR,
        "extra_globs": SCALAR,
    }
)
EVAL_TRAIN_OUTPUTS_SCHEMA = ObjectSchema(
    {
        "required": SCALAR,
        "checkpoint_policy": SCALAR,
        "explicit_checkpoint": SCALAR,
    }
)
EVAL_SCHEMA = ObjectSchema(
    {
        "enabled": SCALAR,
        "command": SCALAR,
        "command_mode": SCALAR,
        "script": SCALAR,
        "external_runtime": EXTERNAL_RUNTIME_SCHEMA,
        "workdir": SCALAR,
        "args": DYNAMIC,
        "pass_run_args": SCALAR,
        "run_args_flag": SCALAR,
        "pass_model_overrides": SCALAR,
        "model_overrides_flag": SCALAR,
        "launch_mode": SCALAR,
        "launcher": LAUNCHER_SCHEMA,
        "train_outputs": EVAL_TRAIN_OUTPUTS_SCHEMA,
    }
)
OUTPUT_SCHEMA = ObjectSchema(
    {
        "base_output_dir": SCALAR,
        "batch_name": SCALAR,
        "dependencies": DYNAMIC,
    }
)
NOTIFY_SCHEMA = ObjectSchema(
    {
        "enabled": SCALAR,
        "email": SCALAR,
        "when": SCALAR,
    }
)
VALIDATION_SCHEMA = ObjectSchema(
    {
        "cli_args": SCALAR,
        "topology_errors": SCALAR,
        "resource_warnings": SCALAR,
        "runtime_preflight": SCALAR,
    }
)
COMMON_EXPERIMENT_SCHEMA = ObjectSchema(
    {
        "project": SCALAR,
        "experiment_name": SCALAR,
        "model": MODEL_SCHEMA,
        "run": RUN_SCHEMA,
        "launcher": LAUNCHER_SCHEMA,
        "cluster": CLUSTER_SCHEMA,
        "env": ENV_SCHEMA,
        "resources": RESOURCES_SCHEMA,
        "artifacts": ARTIFACTS_SCHEMA,
        "eval": EVAL_SCHEMA,
        "output": OUTPUT_SCHEMA,
        "notify": NOTIFY_SCHEMA,
        "validation": VALIDATION_SCHEMA,
    }
)


def build_config_schema(
    *,
    model_source_key: str,
    model_source_schema: ObjectSchema,
    include_sweep: bool,
) -> ObjectSchema:
    fields = dict(COMMON_EXPERIMENT_SCHEMA.fields)
    fields[model_source_key] = model_source_schema
    if include_sweep:
        fields["sweep"] = SCALAR
    return ObjectSchema(fields)


AUTHORING_SCHEMA = build_config_schema(
    model_source_key="model_registry",
    model_source_schema=MODEL_REGISTRY_SCHEMA,
    include_sweep=True,
)
REPLAY_SCHEMA = build_config_schema(
    model_source_key=REPLAY_MODEL_CATALOG_KEY,
    model_source_schema=MODEL_CATALOG_SCHEMA,
    include_sweep=False,
)
