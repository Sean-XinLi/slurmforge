from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from ..errors import RuntimeContractError
from ..inputs.verifier import verify_and_write_stage_instance_inputs
from ..io import write_json_object
from ..runtime.probe import require_runtime_contract
from .attempt import ExecutionAttempt
from .bindings import bindings_from_file
from .environment import build_execution_env
from .launcher import build_shell_script


@dataclass(frozen=True)
class StageUserCommandResult:
    exit_code: int
    failure_class: str | None
    reason: str
    workdir: Path


def run_stage_user_command(attempt: ExecutionAttempt) -> StageUserCommandResult:
    instance = attempt.instance
    try:
        runtime_report = require_runtime_contract(instance.runtime_plan)
    except RuntimeContractError as exc:
        runtime_report = exc.report  # type: ignore[attr-defined]
        write_json_object(attempt.attempt_dir / "runtime_probe.json", runtime_report)
        raise
    write_json_object(attempt.attempt_dir / "runtime_probe.json", runtime_report)
    bindings = bindings_from_file(attempt.run_dir)
    verify_and_write_stage_instance_inputs(
        instance, bindings, phase="executor", run_dir=attempt.run_dir
    )
    env = build_execution_env(
        instance, bindings, run_dir=attempt.run_dir, attempt_dir=attempt.attempt_dir
    )
    workdir = Path(instance.entry.workdir)
    shell_script = build_shell_script(instance, bindings)
    with (
        attempt.stdout_path.open("w", encoding="utf-8") as stdout,
        attempt.stderr_path.open("w", encoding="utf-8") as stderr,
    ):
        proc = subprocess.run(
            ["bash", "-lc", shell_script],
            cwd=workdir,
            env=env,
            stdout=stdout,
            stderr=stderr,
        )
    exit_code = int(proc.returncode)
    return StageUserCommandResult(
        exit_code=exit_code,
        failure_class=_failure_class(exit_code),
        reason="" if exit_code == 0 else f"stage command exited with code {exit_code}",
        workdir=workdir,
    )


def _failure_class(exit_code: int | None) -> str | None:
    if exit_code in (None, 0):
        return None
    if exit_code == 137:
        return "oom"
    return "script_error"
