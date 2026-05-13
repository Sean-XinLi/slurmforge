from __future__ import annotations

import sys

from tests.helpers.overlays import apply_overlay


def with_current_python(extra: dict | None = None) -> dict:
    runtime = {
        "runtime": {
            "executor": {"python": {"bin": sys.executable}},
            "user": {"default": {"python": {"bin": sys.executable}}},
        }
    }
    return apply_overlay(runtime, extra or {})


def grid_runs(count: int) -> dict:
    return {
        "runs": {
            "type": "grid",
            "axes": {"train.entry.args.lr": [0.001 + index for index in range(count)]},
        }
    }


def terminal_email_overlay(recipients: list[str]) -> dict:
    return {
        "notifications": {
            "email": {
                "enabled": True,
                "recipients": recipients,
                "events": ["train_eval_pipeline_finished"],
                "when": "afterany",
            }
        }
    }
