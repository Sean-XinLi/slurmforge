from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any

from ..errors import RuntimeContractError
from ..io import SchemaVersion


@dataclass(frozen=True)
class RuntimeProbeRecord:
    runtime_role: str
    python_bin: str
    min_version: str
    state: str
    runtime_name: str = ""
    python_version: str = ""
    executable: str = ""
    reason: str = ""
    schema_version: int = SchemaVersion.RUNTIME_CONTRACT


@dataclass(frozen=True)
class RuntimeContractReport:
    state: str
    probes: tuple[RuntimeProbeRecord, ...]
    failure_reason: str = ""
    schema_version: int = SchemaVersion.RUNTIME_CONTRACT


def _version_tuple(value: str) -> tuple[int, ...]:
    parts: list[int] = []
    for item in value.split("."):
        if not item:
            continue
        digits = "".join(ch for ch in item if ch.isdigit())
        if digits:
            parts.append(int(digits))
    return tuple(parts)


def probe_python_runtime(
    python_bin: str,
    *,
    min_version: str = "3.10",
    runtime_role: str = "python",
    runtime_name: str = "",
) -> RuntimeProbeRecord:
    resolved = shutil.which(python_bin) if not python_bin.startswith("/") else python_bin
    if not resolved:
        return RuntimeProbeRecord(
            runtime_role=runtime_role,
            python_bin=python_bin,
            min_version=min_version,
            state="failed",
            runtime_name=runtime_name,
            reason=f"python binary was not found: {python_bin}",
        )
    code = (
        "import json,sys;"
        "print(json.dumps({'version': '.'.join(map(str, sys.version_info[:3])), 'executable': sys.executable}))"
    )
    try:
        proc = subprocess.run(
            [resolved, "-c", code],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        return RuntimeProbeRecord(
            runtime_role=runtime_role,
            python_bin=python_bin,
            min_version=min_version,
            state="failed",
            runtime_name=runtime_name,
            executable=str(resolved),
            reason=str(exc),
        )
    if proc.returncode != 0:
        return RuntimeProbeRecord(
            runtime_role=runtime_role,
            python_bin=python_bin,
            min_version=min_version,
            state="failed",
            runtime_name=runtime_name,
            executable=str(resolved),
            reason=(proc.stderr or proc.stdout).strip(),
        )
    payload = json.loads(proc.stdout)
    version = str(payload["version"])
    state = "verified" if _version_tuple(version) >= _version_tuple(min_version) else "failed"
    reason = "verified" if state == "verified" else f"python {version} is below required {min_version}"
    return RuntimeProbeRecord(
        runtime_role=runtime_role,
        python_bin=python_bin,
        min_version=min_version,
        state=state,
        runtime_name=runtime_name,
        python_version=version,
        executable=str(payload.get("executable") or resolved),
        reason=reason,
    )


def probe_runtime_plan(runtime_plan: dict[str, Any]) -> tuple[RuntimeProbeRecord, ...]:
    probes: list[RuntimeProbeRecord] = []
    executor_plan = dict(runtime_plan.get("executor") or {})
    executor_python = dict(executor_plan.get("python") or {})
    if executor_python:
        probes.append(
            probe_python_runtime(
                str(executor_python.get("bin") or "python3"),
                min_version=str(executor_python.get("min_version") or "3.10"),
                runtime_role="executor",
            )
        )
    user_plan = dict(runtime_plan.get("user") or {})
    user_python = dict(user_plan.get("python") or {})
    if user_python:
        probes.append(
            probe_python_runtime(
                str(user_python.get("bin") or "python3"),
                min_version=str(user_python.get("min_version") or "3.10"),
                runtime_role="user",
                runtime_name=str(user_plan.get("name") or "default"),
            )
        )
    return tuple(probes)


def _runtime_probe_failures(probes: tuple[RuntimeProbeRecord, ...]) -> tuple[RuntimeProbeRecord, ...]:
    return tuple(probe for probe in probes if probe.state != "verified")


def _runtime_contract_failure_reason(probes: tuple[RuntimeProbeRecord, ...]) -> str:
    failures = _runtime_probe_failures(probes)
    if not failures:
        return ""
    details = []
    for probe in failures:
        name = f"{probe.runtime_role}:{probe.runtime_name}" if probe.runtime_name else probe.runtime_role
        details.append(f"{name} python `{probe.python_bin}`: {probe.reason}")
    return "runtime contract failed: " + "; ".join(details)


def check_runtime_contract(runtime_plan: dict[str, Any]) -> RuntimeContractReport:
    probes = probe_runtime_plan(runtime_plan)
    failure_reason = _runtime_contract_failure_reason(probes)
    return RuntimeContractReport(
        state="failed" if failure_reason else "verified",
        probes=probes,
        failure_reason=failure_reason,
    )


def require_runtime_contract(runtime_plan: dict[str, Any]) -> RuntimeContractReport:
    report = check_runtime_contract(runtime_plan)
    if report.state != "verified":
        exc = RuntimeContractError(report.failure_reason)
        exc.report = report  # type: ignore[attr-defined]
        raise exc
    return report
