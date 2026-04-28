from __future__ import annotations

from typing import Any

from ..defaults import (
    DEFAULT_EXPERIMENT,
    DEFAULT_PARTITION,
    DEFAULT_PROJECT,
    DEFAULT_PYTHON_BIN,
    DEFAULT_STORAGE_ROOT,
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
