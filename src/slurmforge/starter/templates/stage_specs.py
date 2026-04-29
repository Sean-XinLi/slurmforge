from __future__ import annotations

from typing import Any

from ...config_contract.defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_EVAL_SCRIPT,
    DEFAULT_INPUT_EXPECTS,
    DEFAULT_INPUT_INJECT_MODE,
    DEFAULT_OUTPUT_DISCOVER_SELECT,
    DEFAULT_STAGE_ENABLED,
    DEFAULT_STAGE_ENTRY_TYPE,
    DEFAULT_STAGE_ENTRY_WORKDIR,
    DEFAULT_STAGE_ENVIRONMENT,
    DEFAULT_STAGE_LAUNCHER_TYPE,
    DEFAULT_STAGE_RUNTIME,
    DEFAULT_TRAIN_SCRIPT,
)
from ...config_contract.options import (
    INPUT_SOURCE_EXTERNAL_PATH,
    INPUT_SOURCE_UPSTREAM_OUTPUT,
    OUTPUT_KIND_FILE,
    OUTPUT_KIND_METRIC,
)
from ...config_contract.workflows import STAGE_EVAL, STAGE_TRAIN
from .resources import stage_resources


def train_stage() -> dict[str, Any]:
    return {
        "kind": STAGE_TRAIN,
        "enabled": DEFAULT_STAGE_ENABLED,
        "environment": DEFAULT_STAGE_ENVIRONMENT,
        "runtime": DEFAULT_STAGE_RUNTIME,
        "entry": {
            "type": DEFAULT_STAGE_ENTRY_TYPE,
            "script": DEFAULT_TRAIN_SCRIPT,
            "workdir": DEFAULT_STAGE_ENTRY_WORKDIR,
            "args": {
                "epochs": 1,
                "lr": 0.001,
            },
        },
        "launcher": {"type": DEFAULT_STAGE_LAUNCHER_TYPE},
        "resources": stage_resources(cpus=4, mem="16G"),
        "outputs": {
            "checkpoint": {
                "kind": OUTPUT_KIND_FILE,
                "required": True,
                "discover": {
                    "globs": ["checkpoints/**/*.pt"],
                    "select": DEFAULT_OUTPUT_DISCOVER_SELECT,
                },
            }
        },
    }


def eval_stage_from_train() -> dict[str, Any]:
    stage = eval_stage_base()
    stage["depends_on"] = [STAGE_TRAIN]
    stage["inputs"] = {
        "checkpoint": {
            "source": {
                "kind": INPUT_SOURCE_UPSTREAM_OUTPUT,
                "stage": STAGE_TRAIN,
                "output": "checkpoint",
            },
            "expects": DEFAULT_INPUT_EXPECTS,
            "required": True,
            "inject": {
                "flag": "checkpoint_path",
                "env": "SFORGE_INPUT_CHECKPOINT",
                "mode": DEFAULT_INPUT_INJECT_MODE,
            },
        }
    }
    return stage


def eval_stage_external_checkpoint() -> dict[str, Any]:
    stage = eval_stage_base()
    stage["inputs"] = {
        "checkpoint": {
            "source": {
                "kind": INPUT_SOURCE_EXTERNAL_PATH,
                "path": DEFAULT_CHECKPOINT_PATH,
            },
            "expects": DEFAULT_INPUT_EXPECTS,
            "required": True,
            "inject": {
                "flag": "checkpoint_path",
                "env": "SFORGE_INPUT_CHECKPOINT",
                "mode": DEFAULT_INPUT_INJECT_MODE,
            },
        }
    }
    return stage


def eval_stage_base() -> dict[str, Any]:
    return {
        "kind": STAGE_EVAL,
        "enabled": DEFAULT_STAGE_ENABLED,
        "environment": DEFAULT_STAGE_ENVIRONMENT,
        "runtime": DEFAULT_STAGE_RUNTIME,
        "entry": {
            "type": DEFAULT_STAGE_ENTRY_TYPE,
            "script": DEFAULT_EVAL_SCRIPT,
            "workdir": DEFAULT_STAGE_ENTRY_WORKDIR,
            "args": {
                "split": "validation",
            },
        },
        "launcher": {"type": DEFAULT_STAGE_LAUNCHER_TYPE},
        "resources": stage_resources(cpus=2, mem="8G"),
        "outputs": {
            "accuracy": {
                "kind": OUTPUT_KIND_METRIC,
                "file": "eval/metrics.json",
                "json_path": "$.accuracy",
                "required": True,
            }
        },
    }
