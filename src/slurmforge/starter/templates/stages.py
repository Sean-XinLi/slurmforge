from __future__ import annotations

from typing import Any

from ..defaults import DEFAULT_CHECKPOINT_PATH, DEFAULT_EVAL_SCRIPT, DEFAULT_TRAIN_SCRIPT
from .resources import stage_resources


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
