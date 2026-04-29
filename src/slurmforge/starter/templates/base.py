from __future__ import annotations

from typing import Any

from ...config_contract.defaults import (
    DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR,
    DEFAULT_ARTIFACT_STORE_FALLBACK_STRATEGY,
    DEFAULT_ARTIFACT_STORE_STRATEGY,
    DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST,
    DEFAULT_CONTROLLER_CPUS,
    DEFAULT_CONTROLLER_ENVIRONMENT,
    DEFAULT_CONTROLLER_MEM,
    DEFAULT_CONTROLLER_TIME_LIMIT,
    DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
    DEFAULT_DISPATCH_OVERFLOW_POLICY,
    DEFAULT_EMAIL_ENABLED,
    DEFAULT_EMAIL_EVENTS,
    DEFAULT_EMAIL_FROM,
    DEFAULT_EMAIL_MODE,
    DEFAULT_EMAIL_SENDMAIL,
    DEFAULT_EMAIL_SUBJECT_PREFIX,
    DEFAULT_ENVIRONMENT_NAME,
    DEFAULT_EXPERIMENT,
    DEFAULT_EXECUTOR_MODULE,
    DEFAULT_PARTITION,
    DEFAULT_PROJECT,
    DEFAULT_PYTHON_BIN,
    DEFAULT_PYTHON_MIN_VERSION,
    DEFAULT_RUN_TYPE,
    DEFAULT_RUNTIME_NAME,
    DEFAULT_STORAGE_ROOT,
)


def base_config() -> dict[str, Any]:
    return {
        "project": DEFAULT_PROJECT,
        "experiment": DEFAULT_EXPERIMENT,
        "storage": {"root": DEFAULT_STORAGE_ROOT},
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
                    "bin": DEFAULT_PYTHON_BIN,
                    "min_version": DEFAULT_PYTHON_MIN_VERSION,
                },
                "module": DEFAULT_EXECUTOR_MODULE,
            },
            "user": {
                DEFAULT_RUNTIME_NAME: {
                    "python": {
                        "bin": DEFAULT_PYTHON_BIN,
                        "min_version": DEFAULT_PYTHON_MIN_VERSION,
                    },
                    "env": {},
                }
            },
        },
        "artifact_store": {
            "strategy": DEFAULT_ARTIFACT_STORE_STRATEGY,
            "fallback_strategy": DEFAULT_ARTIFACT_STORE_FALLBACK_STRATEGY,
            "verify_digest": DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST,
            "fail_on_verify_error": DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR,
        },
        "notifications": {
            "email": {
                "enabled": DEFAULT_EMAIL_ENABLED,
                "to": [],
                "on": list(DEFAULT_EMAIL_EVENTS),
                "mode": DEFAULT_EMAIL_MODE,
                "from": DEFAULT_EMAIL_FROM,
                "sendmail": DEFAULT_EMAIL_SENDMAIL,
                "subject_prefix": DEFAULT_EMAIL_SUBJECT_PREFIX,
            }
        },
        "runs": {"type": DEFAULT_RUN_TYPE},
        "dispatch": {
            "max_available_gpus": DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
            "overflow_policy": DEFAULT_DISPATCH_OVERFLOW_POLICY,
        },
        "orchestration": {
            "controller": {
                "partition": DEFAULT_PARTITION,
                "cpus": DEFAULT_CONTROLLER_CPUS,
                "mem": DEFAULT_CONTROLLER_MEM,
                "time_limit": DEFAULT_CONTROLLER_TIME_LIMIT,
                "environment": DEFAULT_CONTROLLER_ENVIRONMENT,
            }
        },
        "stages": {},
    }
