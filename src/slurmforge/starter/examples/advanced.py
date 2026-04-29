from __future__ import annotations

from typing import Any

from ...config_contract.defaults import (
    AUTO_VALUE,
    DEFAULT_EXECUTOR_MODULE,
    DEFAULT_LAUNCHER_NNODES,
    DEFAULT_LAUNCHER_NPROC_PER_NODE,
    DEFAULT_PYTHON_MIN_VERSION,
    DEFAULT_RENDEZVOUS_BACKEND,
    DEFAULT_RENDEZVOUS_ENDPOINT,
    DEFAULT_RENDEZVOUS_PORT,
    DEFAULT_STAGE_ENABLED,
    DEFAULT_STAGE_ENTRY_WORKDIR,
    DEFAULT_STAGE_ENVIRONMENT,
    DEFAULT_STAGE_RUNTIME,
)
from ...config_contract.options import (
    ARTIFACT_STRATEGY_COPY,
    ARTIFACT_STRATEGY_HARDLINK,
    DISPATCH_POLICY_SERIALIZE_GROUPS,
    EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED,
    EMAIL_MODE_SUMMARY,
    ENTRY_PYTHON_SCRIPT,
    GPU_SIZING_ESTIMATOR_HEURISTIC,
    INPUT_EXPECTS_PATH,
    INPUT_INJECT_PATH,
    INPUT_SOURCE_UPSTREAM_OUTPUT,
    LAUNCHER_MODE_MULTI_NODE,
    LAUNCHER_SINGLE,
    LAUNCHER_TORCHRUN,
    OUTPUT_KIND_FILE,
    OUTPUT_KIND_METRIC,
    OUTPUT_SELECT_LATEST_STEP,
    RUN_MATRIX,
)
from ...config_contract.starter_io import (
    ACCURACY_FILE,
    ACCURACY_JSON_PATH,
    ACCURACY_OUTPUT_NAME,
    CHECKPOINT_ENV,
    CHECKPOINT_FLAG,
    CHECKPOINT_GLOB,
    CHECKPOINT_INPUT_NAME,
    CHECKPOINT_OUTPUT_NAME,
    EVAL_SPLIT_DEFAULT,
)
from ...config_contract.workflows import STAGE_EVAL, STAGE_TRAIN


def advanced_example_config() -> dict[str, Any]:
    return {
        "project": "resnet",
        "experiment": "ablation_matrix",
        "storage": {"root": "/shared/runs"},
        "hardware": {
            "gpu_types": {
                "a100_80gb": {
                    "memory_gb": 80,
                    "usable_memory_fraction": 0.9,
                    "max_gpus_per_node": 8,
                    "slurm": {"constraint": "a100"},
                }
            }
        },
        "environments": {
            DEFAULT_STAGE_ENVIRONMENT: {
                "modules": ["cuda/12.1"],
                "source": [{"path": "/shared/envs/slurmforge.sh", "args": ["train"]}],
                "env": {"HF_HOME": "/shared/cache/huggingface"},
            }
        },
        "runtime": {
            "executor": {
                "python": {
                    "bin": "python3",
                    "min_version": DEFAULT_PYTHON_MIN_VERSION,
                },
                "module": DEFAULT_EXECUTOR_MODULE,
            },
            "user": {
                DEFAULT_STAGE_RUNTIME: {
                    "python": {
                        "bin": "/shared/envs/train/bin/python",
                        "min_version": DEFAULT_PYTHON_MIN_VERSION,
                    },
                    "env": {"TOKENIZERS_PARALLELISM": "false"},
                }
            },
        },
        "sizing": {"gpu": {"defaults": {"safety_factor": 1.15, "round_to": 1}}},
        "artifact_store": {
            "strategy": ARTIFACT_STRATEGY_HARDLINK,
            "fallback_strategy": ARTIFACT_STRATEGY_COPY,
            "verify_digest": True,
            "fail_on_verify_error": True,
        },
        "notifications": {
            "email": {
                "enabled": True,
                "to": ["ml-team@example.com"],
                "on": [EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED],
                "mode": EMAIL_MODE_SUMMARY,
                "from": "slurmforge@example.com",
                "sendmail": "/usr/sbin/sendmail",
                "subject_prefix": "SlurmForge",
            }
        },
        "runs": _advanced_runs(),
        "dispatch": {
            "max_available_gpus": 16,
            "overflow_policy": DISPATCH_POLICY_SERIALIZE_GROUPS,
        },
        "orchestration": {
            "controller": {
                "partition": "gpu",
                "cpus": 1,
                "mem": "2G",
                "time_limit": "12:00:00",
                "environment": DEFAULT_STAGE_ENVIRONMENT,
            }
        },
        "stages": {
            STAGE_TRAIN: _advanced_train_stage(),
            STAGE_EVAL: _advanced_eval_stage(),
        },
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


def _advanced_train_stage() -> dict[str, Any]:
    return {
        "kind": STAGE_TRAIN,
        "enabled": DEFAULT_STAGE_ENABLED,
        "environment": DEFAULT_STAGE_ENVIRONMENT,
        "runtime": DEFAULT_STAGE_RUNTIME,
        "before": [{"name": "prepare_cache", "run": 'mkdir -p "$HF_HOME"'}],
        "entry": {
            "type": ENTRY_PYTHON_SCRIPT,
            "script": "train.py",
            "workdir": DEFAULT_STAGE_ENTRY_WORKDIR,
            "args": {
                "epochs": 5,
                "batch_size": 64,
            },
        },
        "launcher": {
            "type": LAUNCHER_TORCHRUN,
            "mode": LAUNCHER_MODE_MULTI_NODE,
            "nnodes": DEFAULT_LAUNCHER_NNODES,
            "nproc_per_node": DEFAULT_LAUNCHER_NPROC_PER_NODE,
            "rendezvous": {
                "backend": DEFAULT_RENDEZVOUS_BACKEND,
                "endpoint": DEFAULT_RENDEZVOUS_ENDPOINT,
                "port": DEFAULT_RENDEZVOUS_PORT,
            },
            "srun_args": ["--cpu-bind=cores"],
        },
        "resources": {
            "partition": "gpu",
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
        },
        "gpu_sizing": {
            "estimator": GPU_SIZING_ESTIMATOR_HEURISTIC,
            "target_memory_gb": 120,
            "min_gpus_per_job": 2,
            "max_gpus_per_job": 16,
            "safety_factor": 1.2,
            "round_to": 2,
        },
        "outputs": {
            CHECKPOINT_OUTPUT_NAME: {
                "kind": OUTPUT_KIND_FILE,
                "required": True,
                "discover": {
                    "globs": [CHECKPOINT_GLOB],
                    "select": OUTPUT_SELECT_LATEST_STEP,
                },
            }
        },
    }


def _advanced_eval_stage() -> dict[str, Any]:
    return {
        "kind": STAGE_EVAL,
        "enabled": DEFAULT_STAGE_ENABLED,
        "depends_on": [STAGE_TRAIN],
        "environment": DEFAULT_STAGE_ENVIRONMENT,
        "runtime": DEFAULT_STAGE_RUNTIME,
        "entry": {
            "type": ENTRY_PYTHON_SCRIPT,
            "script": "eval.py",
            "workdir": DEFAULT_STAGE_ENTRY_WORKDIR,
            "args": {"split": EVAL_SPLIT_DEFAULT},
        },
        "launcher": {"type": LAUNCHER_SINGLE},
        "resources": {
            "partition": "gpu",
            "time_limit": "01:00:00",
            "gpu_type": "a100_80gb",
            "nodes": 1,
            "gpus_per_node": 1,
            "cpus_per_task": 4,
            "mem": "32G",
        },
        "inputs": {
            CHECKPOINT_INPUT_NAME: {
                "source": {
                    "kind": INPUT_SOURCE_UPSTREAM_OUTPUT,
                    "stage": STAGE_TRAIN,
                    "output": CHECKPOINT_OUTPUT_NAME,
                },
                "expects": INPUT_EXPECTS_PATH,
                "required": True,
                "inject": {
                    "flag": CHECKPOINT_FLAG,
                    "env": CHECKPOINT_ENV,
                    "mode": INPUT_INJECT_PATH,
                },
            }
        },
        "outputs": {
            ACCURACY_OUTPUT_NAME: {
                "kind": OUTPUT_KIND_METRIC,
                "required": True,
                "file": ACCURACY_FILE,
                "json_path": ACCURACY_JSON_PATH,
            }
        },
    }
