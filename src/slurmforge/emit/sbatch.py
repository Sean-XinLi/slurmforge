from __future__ import annotations

import re
import shlex
from typing import Any


def _q(value: str) -> str:
    return shlex.quote(str(value))


def _job_name(*parts: str) -> str:
    raw = "-".join(part for part in parts if part)
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", raw)[:128] or "sforge"


def _runtime_bootstrap_lines(runtime_plan: dict[str, Any]) -> list[str]:
    runtime_plan = dict(runtime_plan.get("executor") or runtime_plan)
    lines: list[str] = []
    if str(runtime_plan.get("bootstrap_scope") or "sbatch") != "sbatch":
        return lines
    for step in runtime_plan.get("bootstrap_steps") or ():
        step = dict(step)
        step_type = str(step.get("type") or "")
        if step_type == "module_load":
            lines.append(f"module load {_q(str(step['name']))}")
        elif step_type == "source":
            args = " ".join(_q(str(item)) for item in step.get("args") or ())
            suffix = f" {args}" if args else ""
            lines.append(f"source {_q(str(step['path']))}{suffix}")
    for key, value in dict(runtime_plan.get("env") or {}).items():
        lines.append(f"export {str(key)}={_q(str(value))}")
    return lines
