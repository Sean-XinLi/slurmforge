from __future__ import annotations

from typing import Any

from ..defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_EVAL_SCRIPT,
    DEFAULT_EXPERIMENT,
    DEFAULT_PARTITION,
    DEFAULT_PROJECT,
    DEFAULT_PYTHON_BIN,
    DEFAULT_STORAGE_ROOT,
    DEFAULT_TRAIN_SCRIPT,
)


def base_config() -> dict[str, Any]:
    return {
        "project": DEFAULT_PROJECT,
        "experiment": DEFAULT_EXPERIMENT,
        "storage": {"root": DEFAULT_STORAGE_ROOT},
        "environments": {
            "default": {
                "modules": [],
                "source": [],
                "env": {},
            }
        },
        "runtime": {
            "executor": {
                "python": {
                    "bin": DEFAULT_PYTHON_BIN,
                    "min_version": "3.10",
                },
                "module": "slurmforge.executor.stage",
            },
            "user": {
                "default": {
                    "python": {
                        "bin": DEFAULT_PYTHON_BIN,
                        "min_version": "3.10",
                    },
                    "env": {},
                }
            },
        },
        "artifact_store": {
            "strategy": "copy",
            "fallback_strategy": None,
            "verify_digest": True,
            "fail_on_verify_error": True,
        },
        "runs": {"type": "single"},
        "dispatch": {
            "max_available_gpus": 1,
            "overflow_policy": "serialize_groups",
        },
        "orchestration": {
            "controller": {
                "partition": DEFAULT_PARTITION,
                "cpus": 1,
                "mem": "2G",
                "time_limit": "01:00:00",
                "environment": "default",
            }
        },
        "stages": {},
    }


def stage_resources(*, cpus: int, mem: str) -> dict[str, Any]:
    return {
        "partition": DEFAULT_PARTITION,
        "nodes": 1,
        "gpus_per_node": 1,
        "cpus_per_task": cpus,
        "mem": mem,
        "time_limit": "01:00:00",
    }


def train_stage() -> dict[str, Any]:
    return {
        "kind": "train",
        "enabled": True,
        "environment": "default",
        "runtime": "default",
        "entry": {
            "type": "python_script",
            "script": DEFAULT_TRAIN_SCRIPT,
            "workdir": ".",
            "args": {
                "epochs": 1,
                "lr": 0.001,
            },
        },
        "launcher": {"type": "single"},
        "resources": stage_resources(cpus=4, mem="16G"),
        "outputs": {
            "checkpoint": {
                "kind": "file",
                "required": True,
                "discover": {
                    "globs": ["checkpoints/**/*.pt"],
                    "select": "latest_step",
                },
            }
        },
    }


def eval_stage_from_train() -> dict[str, Any]:
    stage = eval_stage_base()
    stage["depends_on"] = ["train"]
    stage["inputs"] = {
        "checkpoint": {
            "source": {
                "kind": "upstream_output",
                "stage": "train",
                "output": "checkpoint",
            },
            "expects": "path",
            "required": True,
            "inject": {
                "flag": "checkpoint_path",
                "env": "SFORGE_INPUT_CHECKPOINT",
                "mode": "path",
            },
        }
    }
    return stage


def eval_stage_external_checkpoint() -> dict[str, Any]:
    stage = eval_stage_base()
    stage["inputs"] = {
        "checkpoint": {
            "source": {
                "kind": "external_path",
                "path": DEFAULT_CHECKPOINT_PATH,
            },
            "expects": "path",
            "required": True,
            "inject": {
                "flag": "checkpoint_path",
                "env": "SFORGE_INPUT_CHECKPOINT",
                "mode": "path",
            },
        }
    }
    return stage


def eval_stage_base() -> dict[str, Any]:
    return {
        "kind": "eval",
        "enabled": True,
        "environment": "default",
        "runtime": "default",
        "entry": {
            "type": "python_script",
            "script": DEFAULT_EVAL_SCRIPT,
            "workdir": ".",
            "args": {
                "split": "validation",
            },
        },
        "launcher": {"type": "single"},
        "resources": stage_resources(cpus=2, mem="8G"),
        "outputs": {
            "accuracy": {
                "kind": "metric",
                "file": "eval/metrics.json",
                "json_path": "$.accuracy",
                "required": True,
            }
        },
    }
