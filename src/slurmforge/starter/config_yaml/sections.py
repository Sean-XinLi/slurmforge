from __future__ import annotations

from typing import Any

from ...config_contract.default_values import (
    DEFAULT_ENVIRONMENT_NAME,
    DEFAULT_RUNTIME_NAME,
)
from ..config_comments import comment_for, inline_comment_for, option_comment
from .scalar import scalar


def render_project(lines: list[str], config: dict[str, Any]) -> None:
    lines.extend(
        [
            comment_for("project", indent=0),
            f"project: {scalar(config['project'])}",
            comment_for("experiment", indent=0),
            f"experiment: {scalar(config['experiment'])}",
            "",
            "storage:",
            comment_for("storage.root", indent=2),
            f"  root: {scalar(config['storage']['root'])}",
            "",
        ]
    )


def render_environments(lines: list[str], config: dict[str, Any]) -> None:
    env = config["environments"][DEFAULT_ENVIRONMENT_NAME]
    lines.extend(
        [
            "environments:",
            f"  {DEFAULT_ENVIRONMENT_NAME}:",
            comment_for("environments.*.modules", indent=4),
            f"    modules: {scalar(env['modules'])}",
            comment_for("environments.*.source", indent=4),
            f"    source: {scalar(env['source'])}",
            comment_for("environments.*.env", indent=4),
            f"    env: {scalar(env['env'])}",
            "",
        ]
    )


def render_runtime(lines: list[str], config: dict[str, Any]) -> None:
    executor = config["runtime"]["executor"]
    executor_python = executor["python"]
    user_default = config["runtime"]["user"][DEFAULT_RUNTIME_NAME]
    user_python = user_default["python"]
    lines.extend(
        [
            "runtime:",
            "  executor:",
            "    python:",
            comment_for("runtime.executor.python.bin", indent=6),
            f"      bin: {scalar(executor_python['bin'])}",
            f"      min_version: {scalar(executor_python['min_version'])}  # {inline_comment_for('runtime.executor.python.min_version')}",
            comment_for("runtime.executor.module", indent=4),
            f"    module: {scalar(executor['module'])}",
            "  user:",
            f"    {DEFAULT_RUNTIME_NAME}:",
            "      python:",
            comment_for("runtime.user.*.python.bin", indent=8),
            f"        bin: {scalar(user_python['bin'])}",
            f"        min_version: {scalar(user_python['min_version'])}  # {inline_comment_for('runtime.user.*.python.min_version')}",
            comment_for("runtime.user.*.env", indent=6),
            f"      env: {scalar(user_default['env'])}",
            "",
        ]
    )


def render_artifact_store(lines: list[str], config: dict[str, Any]) -> None:
    store = config["artifact_store"]
    lines.extend(
        [
            "artifact_store:",
            option_comment("artifact_store.strategy", indent=2),
            f"  strategy: {scalar(store['strategy'])}",
            option_comment("artifact_store.fallback_strategy", indent=2),
            f"  fallback_strategy: {scalar(store['fallback_strategy'])}",
            comment_for("artifact_store.verify_digest", indent=2),
            f"  verify_digest: {scalar(store['verify_digest'])}",
            comment_for("artifact_store.fail_on_verify_error", indent=2),
            f"  fail_on_verify_error: {scalar(store['fail_on_verify_error'])}",
            "",
        ]
    )


def render_notifications(lines: list[str], config: dict[str, Any]) -> None:
    email = config["notifications"]["email"]
    lines.extend(
        [
            "notifications:",
            "  email:",
            comment_for("notifications.email.enabled", indent=4),
            f"    enabled: {scalar(email['enabled'])}",
            comment_for("notifications.email.recipients", indent=4),
            f"    recipients: {scalar(email['recipients'])}",
            option_comment("notifications.email.events", indent=4),
            "    events:",
        ]
    )
    for event in email["events"]:
        lines.append(f"      - {scalar(event)}")
    lines.extend(
        [
            option_comment("notifications.email.when", indent=4),
            f"    when: {scalar(email['when'])}",
            "",
        ]
    )


def render_runs(lines: list[str], config: dict[str, Any]) -> None:
    runs = config["runs"]
    lines.extend(
        [
            "runs:",
            option_comment("runs.type", indent=2),
            f"  type: {scalar(runs['type'])}",
            "",
        ]
    )


def render_dispatch(lines: list[str], config: dict[str, Any]) -> None:
    dispatch = config["dispatch"]
    lines.extend(
        [
            "dispatch:",
            comment_for("dispatch.max_available_gpus", indent=2),
            f"  max_available_gpus: {scalar(dispatch['max_available_gpus'])}",
            option_comment("dispatch.overflow_policy", indent=2),
            f"  overflow_policy: {scalar(dispatch['overflow_policy'])}",
            "",
        ]
    )


def render_orchestration(lines: list[str], config: dict[str, Any]) -> None:
    control = config["orchestration"]["control"]
    lines.extend(
        [
            "orchestration:",
            "  control:",
            comment_for("orchestration.control.partition", indent=4),
            f"    partition: {scalar(control['partition'])}",
            comment_for("orchestration.control.cpus", indent=4),
            f"    cpus: {scalar(control['cpus'])}",
            comment_for("orchestration.control.mem", indent=4),
            f"    mem: {scalar(control['mem'])}",
            comment_for("orchestration.control.time_limit", indent=4),
            f"    time_limit: {scalar(control['time_limit'])}",
            comment_for("orchestration.control.environment", indent=4),
            f"    environment: {scalar(control['environment'])}",
            "",
        ]
    )
