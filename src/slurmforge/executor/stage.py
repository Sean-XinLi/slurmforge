from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from ..errors import InputContractError, RuntimeContractError
from ..inputs import verify_and_write_stage_instance_inputs
from ..io import SchemaVersion, content_digest, read_json, utc_now, write_json
from ..outputs import ArtifactIntegrityError, discover_stage_outputs, write_stage_outputs_record
from ..root_model import refresh_stage_batch_status, refresh_train_eval_pipeline_status
from ..runtime import require_runtime_contract
from ..status import StageAttemptRecord, StageStatusRecord, commit_attempt, commit_stage_status
from .bindings import bindings_from_file
from .environment import build_execution_env
from .instances import find_stage_instance, load_stage_instance_from_run_dir
from .launcher import build_shell_script


def _failure_class(exit_code: int | None) -> str | None:
    if exit_code in (None, 0):
        return None
    if exit_code == 137:
        return "oom"
    return "script_error"


def _next_attempt_id(run_dir: Path) -> str:
    attempts = run_dir / "attempts"
    if not attempts.exists():
        return "0001"
    existing = [int(path.name) for path in attempts.iterdir() if path.is_dir() and path.name.isdigit()]
    return f"{(max(existing) + 1) if existing else 1:04d}"


def _stage_outputs_path(run_dir: Path) -> Path:
    return run_dir / "stage_outputs.json"


def execute_stage_instance(run_dir: Path) -> int:
    instance = load_stage_instance_from_run_dir(run_dir)
    attempt_id = _next_attempt_id(run_dir)
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
    write_json(attempt_dir / "environment_plan.json", instance.environment_plan)
    write_json(attempt_dir / "before_steps.json", {"steps": instance.before_steps})
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
        bindings = bindings_from_file(run_dir)
        verify_and_write_stage_instance_inputs(instance, bindings, phase="executor", run_dir=run_dir)
        env = build_execution_env(instance, bindings, run_dir=run_dir, attempt_dir=attempt_dir)
        workdir = Path(instance.entry.workdir)
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
    if status_state == "success" and _stage_outputs_path(run_dir).exists():
        output_digest = content_digest(read_json(_stage_outputs_path(run_dir)))
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
    instance = find_stage_instance(batch_root, group_index, task_index)
    result = execute_stage_instance(batch_root / instance.run_dir_rel)
    refresh_stage_batch_status(batch_root)
    pipeline_root = batch_root.parent.parent if batch_root.parent.name == "stage_batches" else None
    if pipeline_root is not None and (pipeline_root / "manifest.json").exists():
        refresh_train_eval_pipeline_status(pipeline_root)
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
