from __future__ import annotations

import shlex
import re

from ..plans.runtime import EnvironmentPlan


def _q(value: str) -> str:
    return shlex.quote(str(value))


def _job_name(*parts: str) -> str:
    raw = "-".join(part for part in parts if part)
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", raw)[:128] or "sforge"


def _environment_lines(environment_plan: EnvironmentPlan) -> list[str]:
    lines: list[str] = []
    for module in environment_plan.modules:
        lines.append(f"module load {_q(str(module))}")
    for source in environment_plan.source:
        args = " ".join(_q(str(item)) for item in source.args)
        suffix = f" {args}" if args else ""
        lines.append(f"source {_q(str(source.path))}{suffix}")
    for key, value in environment_plan.env.items():
        lines.append(f"export {str(key)}={_q(str(value))}")
    return lines
