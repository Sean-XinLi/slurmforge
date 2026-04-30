from __future__ import annotations

from typing import Any

from ...config_contract.options import (
    INPUT_SOURCE_EXTERNAL_PATH,
    INPUT_SOURCE_UPSTREAM_OUTPUT,
    OUTPUT_KIND_FILE,
    OUTPUT_KIND_METRIC,
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
from ...config_contract.registry import default_for
from ...config_contract.workflows import STAGE_EVAL, STAGE_TRAIN
from .resources import stage_resources


def train_stage() -> dict[str, Any]:
    return {
        "kind": STAGE_TRAIN,
        "enabled": default_for("stages.*.enabled"),
        "environment": default_for("stages.*.environment"),
        "runtime": default_for("stages.*.runtime"),
        "entry": {
            "type": default_for("stages.*.entry.type"),
            "script": default_for("stages.train.entry.script"),
            "workdir": default_for("stages.*.entry.workdir"),
            "args": {
                "epochs": 1,
                "lr": 0.001,
            },
        },
        "launcher": {"type": default_for("stages.*.launcher.type")},
        "resources": stage_resources(cpus=4, mem="16G"),
        "outputs": {
            CHECKPOINT_OUTPUT_NAME: {
                "kind": OUTPUT_KIND_FILE,
                "required": True,
                "discover": {
                    "globs": [CHECKPOINT_GLOB],
                    "select": default_for("stages.*.outputs.*.discover.select"),
                },
            }
        },
    }


def eval_stage_from_train() -> dict[str, Any]:
    stage = eval_stage_base()
    stage["depends_on"] = [STAGE_TRAIN]
    stage["inputs"] = {
        CHECKPOINT_INPUT_NAME: {
            "source": {
                "kind": INPUT_SOURCE_UPSTREAM_OUTPUT,
                "stage": STAGE_TRAIN,
                "output": CHECKPOINT_OUTPUT_NAME,
            },
            "expects": default_for("stages.*.inputs.*.expects"),
            "required": True,
            "inject": {
                "flag": CHECKPOINT_FLAG,
                "env": CHECKPOINT_ENV,
                "mode": default_for("stages.*.inputs.*.inject.mode"),
            },
        }
    }
    return stage


def eval_stage_external_checkpoint() -> dict[str, Any]:
    stage = eval_stage_base()
    stage["inputs"] = {
        CHECKPOINT_INPUT_NAME: {
            "source": {
                "kind": INPUT_SOURCE_EXTERNAL_PATH,
                "path": default_for("stages.*.inputs.*.source.path"),
            },
            "expects": default_for("stages.*.inputs.*.expects"),
            "required": True,
            "inject": {
                "flag": CHECKPOINT_FLAG,
                "env": CHECKPOINT_ENV,
                "mode": default_for("stages.*.inputs.*.inject.mode"),
            },
        }
    }
    return stage


def eval_stage_base() -> dict[str, Any]:
    return {
        "kind": STAGE_EVAL,
        "enabled": default_for("stages.*.enabled"),
        "environment": default_for("stages.*.environment"),
        "runtime": default_for("stages.*.runtime"),
        "entry": {
            "type": default_for("stages.*.entry.type"),
            "script": default_for("stages.eval.entry.script"),
            "workdir": default_for("stages.*.entry.workdir"),
            "args": {
                "split": EVAL_SPLIT_DEFAULT,
            },
        },
        "launcher": {"type": default_for("stages.*.launcher.type")},
        "resources": stage_resources(cpus=2, mem="8G"),
        "outputs": {
            ACCURACY_OUTPUT_NAME: {
                "kind": OUTPUT_KIND_METRIC,
                "file": ACCURACY_FILE,
                "json_path": ACCURACY_JSON_PATH,
                "required": True,
            }
        },
    }
