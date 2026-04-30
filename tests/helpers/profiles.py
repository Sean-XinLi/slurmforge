from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


DEFAULT_PROFILE = "stage_batch_default"
DEFAULT_REPLACE_SECTIONS = ("runs",)
PROFILE_REPLACE_SECTIONS = (
    "storage",
    "environments.default.env",
    "runs",
    "stages.train.entry.args",
    "stages.eval.entry.args",
    "stages.eval.outputs",
)


def profile_overlay(profile: str, root: Path) -> dict[str, Any]:
    if profile == DEFAULT_PROFILE:
        return _stage_batch_default_overlay(root)
    raise ValueError(f"Unknown demo project profile: {profile}")


def _stage_batch_default_overlay(root: Path) -> dict[str, Any]:
    return {
        "experiment": "stage_pipeline",
        "storage": {"root": str(root / "runs")},
        "environments": {
            "default": {
                "env": {"DEMO_ENV": "1"},
            },
        },
        "runs": {
            "type": "grid",
            "axes": {"train.entry.args.lr": [0.001]},
        },
        "stages": {
            "train": {
                "entry": {
                    "workdir": str(root),
                    "args": {"lr": 0.001},
                },
                "resources": {
                    "nodes": 1,
                    "gpus_per_node": 1,
                    "cpus_per_task": 1,
                    "constraint": "base",
                },
            },
            "eval": {
                "entry": {
                    "workdir": str(root),
                    "args": {},
                },
                "outputs": {
                    "eval_csv": {
                        "kind": "files",
                        "discover": {"globs": ["eval/**/*.csv"]},
                    },
                },
                "resources": {
                    "nodes": 1,
                    "gpus_per_node": 1,
                    "cpus_per_task": 1,
                },
            },
        },
        "orchestration": {
            "control": {
                "partition": "cpu",
                "cpus": 1,
                "mem": "2G",
                "time_limit": "01:00:00",
                "environment": "",
            },
        },
        "runtime": {
            "executor": {"python": {"bin": sys.executable}},
            "user": {"default": {"python": {"bin": sys.executable}}},
        },
    }
