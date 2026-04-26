from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

from ..errors import InputContractError, RuntimeContractError
from ..schema import InputBinding, input_binding_from_dict, input_injection_value
from ..inputs import verify_and_write_stage_instance_inputs
from ..io import SchemaVersion, content_digest, read_json, utc_now, write_json
from ..outputs import ArtifactIntegrityError, discover_stage_outputs, write_stage_outputs_record
from ..plans import (
    StageInstancePlan,
    stage_instance_plan_from_dict,
)
from ..runtime import require_runtime_contract
from ..status import StageAttemptRecord, StageStatusRecord, commit_attempt, commit_stage_status
from ..storage import (
    input_bindings_path,
    load_execution_stage_batch_plan,
    next_attempt_id,
    refresh_pipeline_status,
    refresh_stage_batch_status,
    stage_outputs_path,
    stage_plan_path,
)


def _find_instance(batch_root: Path, group_index: int, task_index: int) -> StageInstancePlan:
    batch = load_execution_stage_batch_plan(batch_root)
    group = next((item for item in batch.group_plans if item.group_index == group_index), None)
    if group is None:
        raise ValueError(f"No group_index={group_index} in {batch_root}")
    if task_index < 0 or task_index >= len(group.stage_instance_ids):
        raise ValueError(f"task_index={task_index} outside group array size {len(group.stage_instance_ids)}")
    target = group.stage_instance_ids[task_index]
    return next(item for item in batch.stage_instances if item.stage_instance_id == target)


def _load_instance_from_run_dir(run_dir: Path) -> StageInstancePlan:
    return stage_instance_plan_from_dict(read_json(stage_plan_path(run_dir)))


def _flag(name: str) -> str:
    return name if name.startswith("-") else f"--{name}"


def _args_to_argv(args: dict[str, Any]) -> list[str]:
    argv: list[str] = []
    for key in sorted(args):
        value = args[key]
        if value is None:
            continue
        flag = _flag(str(key).replace("_", "-"))
        if isinstance(value, bool):
            argv.append(flag)
            if not value:
                argv.append("false")
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                argv.extend([flag, str(item)])
            continue
        argv.extend([flag, str(value)])
    return argv


def _bindings_from_file(run_dir: Path) -> tuple[InputBinding, ...]:
    payload = read_json(input_bindings_path(run_dir))
    if "schema_version" not in payload:
        raise ValueError("input_bindings.schema_version is required")
    if int(payload["schema_version"]) != 1:
        raise ValueError(f"input_bindings.schema_version is not supported: {payload['schema_version']}")
    return tuple(input_binding_from_dict(dict(item)) for item in dict(payload.get("bindings") or {}).values())


def _build_env(
    instance: StageInstancePlan,
    bindings: tuple[InputBinding, ...],
    *,
    run_dir: Path,
    attempt_dir: Path,
) -> dict[str, str]:
    env = os.environ.copy()
    executor_plan = dict(instance.runtime_plan.get("executor") or {})
    for key, value in dict(executor_plan.get("env") or {}).items():
        env[str(key)] = str(value)
    user_plan = dict(instance.runtime_plan.get("user") or {})
    for key, value in dict(user_plan.get("env") or {}).items():
        env[str(key)] = str(value)
    env["SFORGE_RUN_ID"] = instance.run_id
    env["SFORGE_STAGE_NAME"] = instance.stage_name
    env["SFORGE_STAGE_INSTANCE_ID"] = instance.stage_instance_id
    env["SFORGE_INPUT_BINDINGS"] = str(input_bindings_path(run_dir))
    env["SFORGE_ATTEMPT_DIR"] = str(attempt_dir)
    for binding in bindings:
        env_name = binding.inject.get("env")
        injected = _binding_injected_value(binding)
        if env_name and injected is not None:
            env[str(env_name)] = injected
    return env


def _binding_injected_value(binding: InputBinding) -> str | None:
    return input_injection_value(binding)


def _env_setup_lines(instance: StageInstancePlan) -> list[str]:
    return ["set -euo pipefail"]


def _build_command(instance: StageInstancePlan, bindings: tuple[InputBinding, ...]) -> tuple[list[str] | str, bool]:
    entry = instance.entry
    extra_args = _args_to_argv(dict(entry.get("args") or {}))
    for binding in bindings:
        injected = _binding_injected_value(binding)
        if binding.inject.get("required") and injected is None:
            raise FileNotFoundError(f"Required input `{binding.input_name}` is unresolved")
        flag = binding.inject.get("flag")
        if flag and injected is not None:
            extra_args.extend([_flag(str(flag)), injected])
    launcher = dict(instance.launcher_plan or {})
    launcher_type = str(launcher.get("type") or "single")
    user_plan = dict(instance.runtime_plan.get("user") or {})
    runtime_python = dict(user_plan.get("python") or {})
    python_bin = str(runtime_python.get("bin") or "python3")
    if entry.get("type") == "python_script":
        base = [python_bin, str(entry["script"]), *extra_args]
        if launcher_type in {"single", "python"}:
            return base, False
        if launcher_type == "torchrun":
            if str(launcher.get("mode") or "single_node") == "multi_node":
                return _torchrun_multi_node_command(
                    python_bin=python_bin,
                    script=str(entry["script"]),
                    script_args=extra_args,
                    launcher=launcher,
                ), True
            torchrun = [
                python_bin,
                "-m",
                "torch.distributed.run",
                "--nnodes",
                str(launcher["nnodes"]),
                "--nproc-per-node",
                str(launcher["nproc_per_node"]),
            ]
            rendezvous = dict(launcher.get("rendezvous") or {})
            if launcher.get("master_port") is not None:
                torchrun.extend(["--master-port", str(launcher["master_port"])])
            elif rendezvous.get("port") is not None:
                torchrun.extend(["--master-port", str(rendezvous["port"])])
            return [*torchrun, str(entry["script"]), *extra_args], False
        if launcher_type == "srun":
            return ["srun", *[str(item) for item in launcher.get("args", ())], *base], False
        if launcher_type == "mpirun":
            return ["mpirun", *[str(item) for item in launcher.get("args", ())], *base], False
        if launcher_type == "command":
            return base, False
        raise ValueError(f"Unsupported launcher type: {launcher_type}")
    command = entry.get("command")
    if isinstance(command, list):
        base = [str(item) for item in command] + extra_args
        if launcher_type == "srun":
            return ["srun", *[str(item) for item in launcher.get("args", ())], *base], False
        if launcher_type == "mpirun":
            return ["mpirun", *[str(item) for item in launcher.get("args", ())], *base], False
        return base, False
    suffix = "" if not extra_args else " " + shlex.join(extra_args)
    command_text = str(command) + suffix
    if launcher_type == "srun":
        args = " ".join(shlex.quote(str(item)) for item in launcher.get("args", ()))
        return f"srun {args} {command_text}".strip(), True
    if launcher_type == "mpirun":
        args = " ".join(shlex.quote(str(item)) for item in launcher.get("args", ()))
        return f"mpirun {args} {command_text}".strip(), True
    return command_text, True


def _torchrun_multi_node_command(
    *,
    python_bin: str,
    script: str,
    script_args: list[str],
    launcher: dict[str, Any],
) -> str:
    rendezvous = dict(launcher.get("rendezvous") or {})
    backend = str(rendezvous.get("backend") or "c10d")
    endpoint = str(rendezvous.get("endpoint") or "auto")
    port = int(rendezvous.get("port") or launcher.get("master_port") or 29500)
    nnodes = int(launcher["nnodes"])
    nproc_per_node = int(launcher["nproc_per_node"])
    if endpoint == "auto":
        endpoint_expr = '"${MASTER_ADDR}:${MASTER_PORT}"'
        prelude = [
            f"MASTER_PORT={shlex.quote(str(port))}",
            'MASTER_ADDR="${MASTER_ADDR:-$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)}"',
            "export MASTER_ADDR MASTER_PORT",
        ]
    else:
        endpoint_expr = shlex.quote(endpoint)
        prelude = [f"MASTER_PORT={shlex.quote(str(port))}", "export MASTER_PORT"]
    inner_parts = [
        'NODE_RANK="${SLURM_PROCID:-0}"',
        "export NODE_RANK",
        "exec",
        shlex.quote(python_bin),
        "-m",
        "torch.distributed.run",
        "--nnodes",
        shlex.quote(str(nnodes)),
        "--nproc-per-node",
        shlex.quote(str(nproc_per_node)),
        "--node-rank",
        '"${NODE_RANK}"',
        "--rdzv-backend",
        shlex.quote(backend),
        "--rdzv-endpoint",
        endpoint_expr,
        shlex.quote(script),
        *(shlex.quote(str(item)) for item in script_args),
    ]
    inner = " ".join(inner_parts)
    srun_args = [str(item) for item in launcher.get("srun_args") or ()]
    srun = [
        "srun",
        "--nodes",
        str(nnodes),
        "--ntasks",
        str(nnodes),
        "--ntasks-per-node",
        "1",
        *srun_args,
        "bash",
        "-lc",
        inner,
    ]
    return "; ".join([*prelude, shlex.join(srun)])


def build_shell_script(instance: StageInstancePlan, bindings: tuple[InputBinding, ...]) -> str:
    command, use_shell = _build_command(instance, bindings)
    if isinstance(command, list):
        command_text = shlex.join(command)
    else:
        command_text = command if use_shell else shlex.quote(command)
    return "\n".join([*_env_setup_lines(instance), command_text])


def _failure_class(exit_code: int | None) -> str | None:
    if exit_code in (None, 0):
        return None
    if exit_code == 137:
        return "oom"
    return "script_error"


def execute_stage_instance(run_dir: Path) -> int:
    instance = _load_instance_from_run_dir(run_dir)
    attempt_id = next_attempt_id(run_dir)
    attempt_dir = run_dir / "attempts" / attempt_id
    log_dir = attempt_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    started = utc_now()
    scheduler_job_id = os.environ.get("SLURM_JOB_ID", "")
    scheduler_array_job_id = os.environ.get("SLURM_ARRAY_JOB_ID", "")
    scheduler_array_task_id = os.environ.get("SLURM_ARRAY_TASK_ID", "")
    skeleton = StageAttemptRecord(
        attempt_id=attempt_id,
        stage_instance_id=instance.stage_instance_id,
        attempt_source="executor",
        attempt_state="running",
        scheduler_job_id=scheduler_job_id,
        scheduler_array_job_id=scheduler_array_job_id,
        scheduler_array_task_id=scheduler_array_task_id,
        node_list=os.environ.get("SLURM_NODELIST", ""),
        started_by_executor=True,
        executor_started_at=started,
        started_at=started,
        reason="executor started",
    )
    commit_attempt(run_dir, skeleton)
    write_json(attempt_dir / "launcher_plan.json", instance.launcher_plan)
    commit_stage_status(
        run_dir,
        StageStatusRecord(
            schema_version=SchemaVersion.STATUS,
            stage_instance_id=instance.stage_instance_id,
            run_id=instance.run_id,
            stage_name=instance.stage_name,
            state="running",
            latest_attempt_id=attempt_id,
        ),
        allow_new_attempt=True,
        source="executor",
    )
    stdout_path = log_dir / f"{instance.stage_name}.out"
    stderr_path = log_dir / f"{instance.stage_name}.err"
    exit_code: int | None = None
    failure_class: str | None = None
    reason = ""
    artifact_paths: list[str] = []
    try:
        try:
            runtime_report = require_runtime_contract(instance.runtime_plan)
        except RuntimeContractError as exc:
            runtime_report = exc.report  # type: ignore[attr-defined]
            write_json(attempt_dir / "runtime_probe.json", runtime_report)
            raise
        write_json(attempt_dir / "runtime_probe.json", runtime_report)
        bindings = _bindings_from_file(run_dir)
        verify_and_write_stage_instance_inputs(instance, bindings, phase="executor", run_dir=run_dir)
        env = _build_env(instance, bindings, run_dir=run_dir, attempt_dir=attempt_dir)
        workdir = Path(instance.entry["workdir"])
        shell_script = build_shell_script(instance, bindings)
        with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
            proc = subprocess.run(["bash", "-lc", shell_script], cwd=workdir, env=env, stdout=stdout, stderr=stderr)
        exit_code = int(proc.returncode)
        failure_class = _failure_class(exit_code)
        if exit_code == 0:
            output_result = discover_stage_outputs(
                instance,
                workdir,
                attempt_id=attempt_id,
                attempt_dir=attempt_dir,
            )
            artifact_paths = list(output_result.artifact_paths)
            if output_result.failure_reason is not None:
                failure_class = "missing_output"
                reason = output_result.failure_reason
            else:
                write_stage_outputs_record(output_result.stage_outputs, run_dir=run_dir, attempt_dir=attempt_dir)
        else:
            reason = f"stage command exited with code {exit_code}"
    except Exception as exc:
        exit_code = 2 if exit_code is None else exit_code
        if isinstance(exc, ArtifactIntegrityError):
            failure_class = "artifact_integrity_error"
        elif isinstance(exc, RuntimeContractError):
            failure_class = "runtime_contract_error"
        elif isinstance(exc, InputContractError):
            failure_class = "input_contract_error"
        else:
            failure_class = failure_class or "executor_error"
        reason = str(exc)
    finished = utc_now()
    attempt = StageAttemptRecord(
        attempt_id=attempt_id,
        stage_instance_id=instance.stage_instance_id,
        attempt_source="executor",
        attempt_state="final",
        scheduler_job_id=scheduler_job_id,
        scheduler_array_job_id=scheduler_array_job_id,
        scheduler_array_task_id=scheduler_array_task_id,
        scheduler_state="",
        scheduler_exit_code="",
        node_list=os.environ.get("SLURM_NODELIST", ""),
        started_by_executor=True,
        executor_started_at=started,
        executor_finished_at=finished,
        started_at=started,
        finished_at=finished,
        exit_code=exit_code,
        failure_class=failure_class,
        reason=reason,
        log_paths=(str(stdout_path), str(stderr_path)),
        artifact_paths=tuple(artifact_paths),
        artifact_manifest_path=str((attempt_dir / "artifacts" / "artifact_manifest.json").resolve())
        if (attempt_dir / "artifacts" / "artifact_manifest.json").exists()
        else "",
    )
    commit_attempt(run_dir, attempt)
    status_state = "success" if exit_code == 0 and failure_class is None else "failed"
    output_digest = None
    if status_state == "success" and stage_outputs_path(run_dir).exists():
        output_digest = content_digest(read_json(stage_outputs_path(run_dir)))
    commit_stage_status(
        run_dir,
        StageStatusRecord(
            schema_version=SchemaVersion.STATUS,
            stage_instance_id=instance.stage_instance_id,
            run_id=instance.run_id,
            stage_name=instance.stage_name,
            state=status_state,
            latest_attempt_id=attempt_id,
            latest_output_digest=output_digest,
            failure_class=failure_class,
            reason=reason,
        ),
        allow_new_attempt=True,
        source="executor",
    )
    return 0 if status_state == "success" else int(exit_code or 1)


def execute_stage_task(batch_root: Path, group_index: int, task_index: int) -> int:
    instance = _find_instance(batch_root, group_index, task_index)
    result = execute_stage_instance(batch_root / instance.run_dir_rel)
    refresh_stage_batch_status(batch_root)
    pipeline_root = batch_root.parent.parent if batch_root.parent.name == "stage_batches" else None
    if pipeline_root is not None and (pipeline_root / "manifest.json").exists():
        refresh_pipeline_status(pipeline_root)
    return result


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Execute one slurmforge stage attempt")
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--group-index", required=True, type=int)
    parser.add_argument("--task-index", required=True, type=int)
    args = parser.parse_args(argv)
    raise SystemExit(execute_stage_task(Path(args.batch_root), args.group_index, args.task_index))


if __name__ == "__main__":
    main(sys.argv[1:])
