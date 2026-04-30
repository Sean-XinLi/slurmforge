from __future__ import annotations

from typing import Any

from ...config_contract.default_values import (
    DEFAULT_ENVIRONMENT_NAME,
    DEFAULT_RUNTIME_NAME,
)
from ...config_contract.registry import default_for


def base_config() -> dict[str, Any]:
    return {
        "project": default_for("project"),
        "experiment": default_for("experiment"),
        "storage": {"root": default_for("storage.root")},
        "environments": {
            DEFAULT_ENVIRONMENT_NAME: {
                "modules": [],
                "source": [],
                "env": {},
            }
        },
        "runtime": {
            "executor": {
                "python": {
                    "bin": default_for("runtime.executor.python.bin"),
                    "min_version": default_for("runtime.executor.python.min_version"),
                },
                "module": default_for("runtime.executor.module"),
            },
            "user": {
                DEFAULT_RUNTIME_NAME: {
                    "python": {
                        "bin": default_for("runtime.user.*.python.bin"),
                        "min_version": default_for("runtime.user.*.python.min_version"),
                    },
                    "env": {},
                }
            },
        },
        "artifact_store": {
            "strategy": default_for("artifact_store.strategy"),
            "fallback_strategy": default_for("artifact_store.fallback_strategy"),
            "verify_digest": default_for("artifact_store.verify_digest"),
            "fail_on_verify_error": default_for("artifact_store.fail_on_verify_error"),
        },
        "notifications": {
            "email": {
                "enabled": default_for("notifications.email.enabled"),
                "recipients": [],
                "events": list(default_for("notifications.email.events")),
                "when": default_for("notifications.email.when"),
            }
        },
        "runs": {"type": default_for("runs.type")},
        "dispatch": {
            "max_available_gpus": default_for("dispatch.max_available_gpus"),
            "overflow_policy": default_for("dispatch.overflow_policy"),
        },
        "orchestration": {
            "control": {
                "partition": default_for("orchestration.control.partition"),
                "cpus": default_for("orchestration.control.cpus"),
                "mem": default_for("orchestration.control.mem"),
                "time_limit": default_for("orchestration.control.time_limit"),
                "environment": default_for("orchestration.control.environment"),
            }
        },
        "stages": {},
    }
