from __future__ import annotations

from typing import Any

from ...config_contract.default_values import AUTO_VALUE
from ...config_contract.option_sets import (
    ARTIFACT_STRATEGY_COPY,
    ARTIFACT_STRATEGY_HARDLINK,
    DISPATCH_POLICY_SERIALIZE_GROUPS,
    EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED,
    EMAIL_WHEN_AFTERANY,
    GPU_SIZING_ESTIMATOR_HEURISTIC,
    LAUNCHER_MODE_MULTI_NODE,
    LAUNCHER_SINGLE,
    LAUNCHER_TORCHRUN,
    RUN_MATRIX,
)
from ...config_contract.registry import default_for
from ...config_contract.workflows import STAGE_EVAL, STAGE_TRAIN
from ..templates.base import base_config
from ..templates.stage_specs import eval_stage_from_train, train_stage


def advanced_example_config() -> dict[str, Any]:
    config = base_config()
    config["project"] = "resnet"
    config["experiment"] = "ablation_matrix"
    config["storage"] = {"root": "/shared/runs"}
    config["hardware"] = _advanced_hardware()
    config["environments"] = _advanced_environments()
    config["runtime"] = _advanced_runtime()
    config["sizing"] = _advanced_sizing()
    config["artifact_store"] = _advanced_artifact_store()
    config["notifications"] = _advanced_notifications()
    config["runs"] = _advanced_runs()
    config["dispatch"] = _advanced_dispatch()
    config["orchestration"] = _advanced_orchestration()
    config["stages"] = {
        STAGE_TRAIN: _advanced_train_stage(),
        STAGE_EVAL: _advanced_eval_stage(),
    }
    return config


def _advanced_hardware() -> dict[str, Any]:
    return {
        "gpu_types": {
            "a100_80gb": {
                "memory_gb": 80,
                "usable_memory_fraction": 0.9,
                "max_gpus_per_node": 8,
                "slurm": {"constraint": "a100"},
            }
        }
    }


def _advanced_environments() -> dict[str, Any]:
    return {
        default_for("stages.*.environment"): {
            "modules": ["cuda/12.1"],
            "source": [{"path": "/shared/envs/slurmforge.sh", "args": ["train"]}],
            "env": {"HF_HOME": "/shared/cache/huggingface"},
        }
    }


def _advanced_runtime() -> dict[str, Any]:
    return {
        "executor": {
            "python": {
                "bin": default_for("runtime.executor.python.bin"),
                "min_version": default_for("runtime.executor.python.min_version"),
            },
            "module": default_for("runtime.executor.module"),
        },
        "user": {
            default_for("stages.*.runtime"): {
                "python": {
                    "bin": "/shared/envs/train/bin/python",
                    "min_version": default_for("runtime.user.*.python.min_version"),
                },
                "env": {"TOKENIZERS_PARALLELISM": "false"},
            }
        },
    }


def _advanced_sizing() -> dict[str, Any]:
    return {"gpu": {"defaults": {"safety_factor": 1.15, "round_to": 1}}}


def _advanced_artifact_store() -> dict[str, Any]:
    return {
        "strategy": ARTIFACT_STRATEGY_HARDLINK,
        "fallback_strategy": ARTIFACT_STRATEGY_COPY,
        "verify_digest": True,
        "fail_on_verify_error": True,
    }


def _advanced_notifications() -> dict[str, Any]:
    return {
        "email": {
            "enabled": True,
            "recipients": ["ml-team@example.com"],
            "events": [EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED],
            "when": EMAIL_WHEN_AFTERANY,
        }
    }


def _advanced_runs() -> dict[str, Any]:
    return {
        "type": RUN_MATRIX,
        "cases": [
            {
                "name": "small",
                "set": {"train.entry.args.model": "resnet18"},
                "axes": {
                    "train.entry.args.lr": [0.001, 0.0005],
                    "train.entry.args.seed": [1, 2],
                },
            },
            {
                "name": "large",
                "set": {
                    "train.entry.args.model": "resnet50",
                    "train.resources.gpu_type": "a100_80gb",
                },
                "axes": {
                    "train.entry.args.lr": [0.0005],
                    "train.entry.args.seed": [1, 2],
                },
            },
        ],
    }


def _advanced_dispatch() -> dict[str, Any]:
    return {
        "max_available_gpus": 16,
        "overflow_policy": DISPATCH_POLICY_SERIALIZE_GROUPS,
    }


def _advanced_orchestration() -> dict[str, Any]:
    return {
        "control": {
            "partition": default_for("orchestration.control.partition"),
            "cpus": default_for("orchestration.control.cpus"),
            "mem": default_for("orchestration.control.mem"),
            "time_limit": default_for("orchestration.control.time_limit"),
            "environment": default_for("orchestration.control.environment"),
        }
    }


def _advanced_train_stage() -> dict[str, Any]:
    stage = train_stage()
    stage["before"] = [{"name": "prepare_cache", "run": 'mkdir -p "$HF_HOME"'}]
    stage["entry"]["args"].update({"epochs": 5, "batch_size": 64})
    stage["launcher"] = {
        "type": LAUNCHER_TORCHRUN,
        "mode": LAUNCHER_MODE_MULTI_NODE,
        "nnodes": default_for("stages.*.launcher.nnodes"),
        "nproc_per_node": default_for("stages.*.launcher.nproc_per_node"),
        "rendezvous": {
            "backend": default_for("stages.*.launcher.rendezvous.backend"),
            "endpoint": default_for("stages.*.launcher.rendezvous.endpoint"),
            "port": default_for("stages.*.launcher.rendezvous.port"),
        },
        "srun_args": ["--cpu-bind=cores"],
    }
    stage["resources"].update(
        {
            "account": "research",
            "qos": "normal",
            "time_limit": "06:00:00",
            "gpu_type": "a100_80gb",
            "nodes": 2,
            "gpus_per_node": AUTO_VALUE,
            "cpus_per_task": 16,
            "mem": "128G",
            "constraint": "a100",
            "extra_sbatch_args": ["--exclusive"],
        }
    )
    stage["gpu_sizing"] = {
        "estimator": GPU_SIZING_ESTIMATOR_HEURISTIC,
        "target_memory_gb": 120,
        "min_gpus_per_job": 2,
        "max_gpus_per_job": 16,
        "safety_factor": 1.2,
        "round_to": 2,
    }
    return stage


def _advanced_eval_stage() -> dict[str, Any]:
    stage = eval_stage_from_train()
    stage["launcher"] = {"type": LAUNCHER_SINGLE}
    stage["resources"].update(
        {
            "time_limit": "01:00:00",
            "gpu_type": "a100_80gb",
            "nodes": 1,
            "gpus_per_node": 1,
            "cpus_per_task": 4,
            "mem": "32G",
        }
    )
    return stage
