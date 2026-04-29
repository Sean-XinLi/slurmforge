from __future__ import annotations

import sys
from pathlib import Path

from ..io import write_exception_diagnostic
from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from .attempt import begin_attempt, complete_attempt
from .finalize import failure_class_for_exception, finalize_successful_stage_outputs
from .instances import find_stage_instance, load_stage_instance_from_run_dir
from .runner import run_stage_user_command


def execute_stage_instance(run_dir: Path) -> int:
    instance = load_stage_instance_from_run_dir(run_dir)
    attempt = begin_attempt(run_dir, instance)
    exit_code: int | None = None
    failure_class: str | None = None
    reason = ""
    artifact_paths: tuple[str, ...] = ()
    try:
        command_result = run_stage_user_command(attempt)
        exit_code = command_result.exit_code
        failure_class = command_result.failure_class
        reason = command_result.reason
        if exit_code == 0:
            output_result = finalize_successful_stage_outputs(
                attempt, command_result.workdir
            )
            artifact_paths = output_result.artifact_paths
            if output_result.failure_class is not None:
                failure_class = output_result.failure_class
                reason = output_result.reason
    except Exception as exc:
        write_exception_diagnostic(attempt.log_dir / "executor_traceback.log", exc)
        exit_code = 2 if exit_code is None else exit_code
        failure_class = failure_class_for_exception(exc, failure_class)
        reason = str(exc)
    return complete_attempt(
        attempt,
        exit_code=exit_code,
        failure_class=failure_class,
        reason=reason,
        artifact_paths=artifact_paths,
    )


def execute_stage_task(batch_root: Path, group_index: int, task_index: int) -> int:
    instance = find_stage_instance(batch_root, group_index, task_index)
    result = execute_stage_instance(batch_root / instance.run_dir_rel)
    refresh_stage_batch_status(batch_root)
    pipeline_root = (
        batch_root.parent.parent if batch_root.parent.name == "stage_batches" else None
    )
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
    raise SystemExit(
        execute_stage_task(Path(args.batch_root), args.group_index, args.task_index)
    )


if __name__ == "__main__":
    main(sys.argv[1:])
