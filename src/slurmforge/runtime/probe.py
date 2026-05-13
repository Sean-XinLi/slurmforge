from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass

from ..config_contract.registry import default_for
from ..errors import RuntimeContractError
from ..io import SchemaVersion
from ..plans.runtime import RuntimePlan


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


def _python_probe_values(stdout: str, *, resolved: str) -> tuple[str, str]:
    payload = json.loads(stdout)
    if not isinstance(payload, dict):
        raise ValueError("probe output must be a JSON object")
    if "version" not in payload:
        raise ValueError("probe output is missing `version`")
    version = payload["version"]
    if not isinstance(version, str) or not version:
        raise ValueError("probe output `version` must be a non-empty string")
    executable = payload["executable"] if "executable" in payload else resolved
    if not isinstance(executable, str) or not executable:
        raise ValueError("probe output `executable` must be a non-empty string")
    return version, executable


def probe_python_runtime(
    python_bin: str,
    *,
    min_version: str | None = None,
    runtime_role: str = "python",
    runtime_name: str = "",
) -> RuntimeProbeRecord:
    required_min_version = str(
        default_for("runtime.executor.python.min_version")
        if min_version is None
        else min_version
    )
    resolved = (
        shutil.which(python_bin) if not python_bin.startswith("/") else python_bin
    )
    if not resolved:
        return RuntimeProbeRecord(
            runtime_role=runtime_role,
            python_bin=python_bin,
            min_version=required_min_version,
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
            min_version=required_min_version,
            state="failed",
            runtime_name=runtime_name,
            executable=str(resolved),
            reason=str(exc),
        )
    if proc.returncode != 0:
        return RuntimeProbeRecord(
            runtime_role=runtime_role,
            python_bin=python_bin,
            min_version=required_min_version,
            state="failed",
            runtime_name=runtime_name,
            executable=str(resolved),
            reason=(proc.stderr or proc.stdout).strip(),
        )
    try:
        version, executable = _python_probe_values(proc.stdout, resolved=str(resolved))
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return RuntimeProbeRecord(
            runtime_role=runtime_role,
            python_bin=python_bin,
            min_version=required_min_version,
            state="failed",
            runtime_name=runtime_name,
            executable=str(resolved),
            reason=f"runtime probe output was invalid: {exc}",
        )
    state = (
        "verified"
        if _version_tuple(version) >= _version_tuple(required_min_version)
        else "failed"
    )
    reason = (
        "verified"
        if state == "verified"
        else f"python {version} is below required {required_min_version}"
    )
    return RuntimeProbeRecord(
        runtime_role=runtime_role,
        python_bin=python_bin,
        min_version=required_min_version,
        state=state,
        runtime_name=runtime_name,
        python_version=version,
        executable=executable,
        reason=reason,
    )


def probe_runtime_plan(runtime_plan: RuntimePlan) -> tuple[RuntimeProbeRecord, ...]:
    probes: list[RuntimeProbeRecord] = []
    default_python_bin = str(default_for("runtime.executor.python.bin"))
    default_min_version = str(default_for("runtime.executor.python.min_version"))
    executor_python = runtime_plan.executor.python
    probes.append(
        probe_python_runtime(
            executor_python.bin or default_python_bin,
            min_version=executor_python.min_version or default_min_version,
            runtime_role="executor",
        )
    )
    user_plan = runtime_plan.user
    if user_plan is not None:
        user_python = user_plan.python
        probes.append(
            probe_python_runtime(
                user_python.bin or default_python_bin,
                min_version=user_python.min_version or default_min_version,
                runtime_role="user",
                runtime_name=user_plan.name or "default",
            )
        )
    return tuple(probes)


def _runtime_probe_failures(
    probes: tuple[RuntimeProbeRecord, ...],
) -> tuple[RuntimeProbeRecord, ...]:
    return tuple(probe for probe in probes if probe.state != "verified")


def _runtime_contract_failure_reason(probes: tuple[RuntimeProbeRecord, ...]) -> str:
    failures = _runtime_probe_failures(probes)
    if not failures:
        return ""
    details = []
    for probe in failures:
        name = (
            f"{probe.runtime_role}:{probe.runtime_name}"
            if probe.runtime_name
            else probe.runtime_role
        )
        details.append(f"{name} python `{probe.python_bin}`: {probe.reason}")
    return "runtime contract failed: " + "; ".join(details)


def check_runtime_contract(runtime_plan: RuntimePlan) -> RuntimeContractReport:
    probes = probe_runtime_plan(runtime_plan)
    failure_reason = _runtime_contract_failure_reason(probes)
    return RuntimeContractReport(
        state="failed" if failure_reason else "verified",
        probes=probes,
        failure_reason=failure_reason,
    )


def require_runtime_contract(runtime_plan: RuntimePlan) -> RuntimeContractReport:
    report = check_runtime_contract(runtime_plan)
    if report.state != "verified":
        exc = RuntimeContractError(report.failure_reason)
        exc.report = report  # type: ignore[attr-defined]
        raise exc
    return report
