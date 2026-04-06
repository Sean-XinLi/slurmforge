from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from slurmforge.pipeline.status import (
    begin_execution_status,
    AttemptResult,
    complete_attempt_result,
    ExecutionStatus,
    finalize_execution_status,
    load_or_infer_execution_status,
    read_attempt_result,
    read_execution_status,
    read_latest_result_dir,
    serialize_attempt_result,
    serialize_execution_status,
    status_path_for_result_dir,
    write_latest_result_dir,
)


def _write_attempt_result(result_dir: Path, payload: AttemptResult) -> None:
    meta_dir = result_dir / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    resolved_payload = complete_attempt_result(payload, result_dir=result_dir)
    (meta_dir / "attempt_result.json").write_text(
        json.dumps(serialize_attempt_result(resolved_payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_execution_status(result_dir: Path, status: ExecutionStatus) -> None:
    path = status_path_for_result_dir(result_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(serialize_execution_status(status), indent=2, sort_keys=True), encoding="utf-8")


class ExecutionStatusTests(unittest.TestCase):
    def test_serialize_execution_status_uses_explicit_schema(self) -> None:
        payload = serialize_execution_status(
            ExecutionStatus(
                state="failed",
                failure_class="script_error",
                failed_stage="train",
                reason="boom",
                train_exit_code=2,
                result_dir="/tmp/run/job-1",
            )
        )
        self.assertEqual(
            set(payload.keys()),
            {
                "schema_version",
                "state",
                "slurm_state",
                "failure_class",
                "failed_stage",
                "reason",
                "train_exit_code",
                "eval_exit_code",
                "shell_exit_code",
                "job_key",
                "slurm_job_id",
                "slurm_array_job_id",
                "slurm_array_task_id",
                "started_at",
                "finished_at",
                "result_dir_rel",
                "train_log_rel",
                "eval_log_rel",
                "slurm_out_rel",
                "slurm_err_rel",
            },
        )
        self.assertNotIn("result_dir", payload)
        self.assertNotIn("train_log", payload)

    def test_finalize_execution_status_classifies_oom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "runs" / "run_001" / "job-123"
            log_dir = result_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            (log_dir / "train.log").write_text("RuntimeError: CUDA out of memory", encoding="utf-8")
            _write_attempt_result(
                result_dir,
                AttemptResult(
                    train_exit_code=1,
                    eval_exit_code=0,
                    job_key="123",
                    slurm_job_id="123",
                    train_log=str(log_dir / "train.log"),
                ),
            )

            status = finalize_execution_status(result_dir=result_dir, started_at="start", shell_exit_code=1)

        self.assertEqual(status.state, "failed")
        self.assertEqual(status.failure_class, "oom")
        self.assertEqual(status.failed_stage, "train")

    def test_finalize_execution_status_classifies_preempted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "runs" / "run_001" / "job-123"
            log_dir = result_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            slurm_err = log_dir / "slurm-123.err"
            slurm_err.write_text("Job requeued due to preemption", encoding="utf-8")
            _write_attempt_result(
                result_dir,
                AttemptResult(
                    train_exit_code=1,
                    eval_exit_code=0,
                    job_key="123",
                    slurm_job_id="123",
                    slurm_err=str(slurm_err),
                ),
            )

            status = finalize_execution_status(result_dir=result_dir, started_at="start", shell_exit_code=1)

        self.assertEqual(status.failure_class, "preempted")

    def test_finalize_execution_status_classifies_node_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "runs" / "run_001" / "job-123"
            log_dir = result_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            slurm_err = log_dir / "slurm-123.err"
            slurm_err.write_text("Batch job aborted: Node failure", encoding="utf-8")
            _write_attempt_result(
                result_dir,
                AttemptResult(
                    train_exit_code=1,
                    eval_exit_code=0,
                    job_key="123",
                    slurm_job_id="123",
                    slurm_err=str(slurm_err),
                ),
            )

            status = finalize_execution_status(result_dir=result_dir, started_at="start", shell_exit_code=1)

        self.assertEqual(status.failure_class, "node_failure")

    def test_finalize_execution_status_classifies_script_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "runs" / "run_001" / "job-123"
            (result_dir / "logs").mkdir(parents=True, exist_ok=True)
            _write_attempt_result(
                result_dir,
                AttemptResult(
                    train_exit_code=2,
                    eval_exit_code=0,
                    job_key="123",
                ),
            )

            status = finalize_execution_status(result_dir=result_dir, started_at="start", shell_exit_code=2)

        self.assertEqual(status.failure_class, "script_error")
        self.assertEqual(status.failed_stage, "train")

    def test_finalize_execution_status_classifies_eval_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "runs" / "run_001" / "job-123"
            (result_dir / "logs").mkdir(parents=True, exist_ok=True)
            _write_attempt_result(
                result_dir,
                AttemptResult(
                    train_exit_code=0,
                    eval_exit_code=7,
                    job_key="123",
                ),
            )

            status = finalize_execution_status(result_dir=result_dir, started_at="start", shell_exit_code=7)

        self.assertEqual(status.failure_class, "eval_failed")
        self.assertEqual(status.failed_stage, "eval")

    def test_load_or_infer_execution_status_converts_stale_running_status_from_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "batch"
            run_dir = batch_root / "runs" / "run_001"
            result_dir = run_dir / "job-123"
            array_log_dir = batch_root / "array_logs"
            array_log_dir.mkdir(parents=True, exist_ok=True)
            (array_log_dir / "slurm-123.err").write_text("Job requeued due to preemption", encoding="utf-8")
            _write_execution_status(
                result_dir,
                ExecutionStatus(
                    state="running",
                    job_key="123",
                    slurm_job_id="123",
                    started_at="start",
                    result_dir=str(result_dir),
                ),
            )
            write_latest_result_dir(run_dir, result_dir)

            status = load_or_infer_execution_status(run_dir)

        self.assertIsNotNone(status)
        self.assertEqual(status.failure_class, "preempted")
        self.assertEqual(status.state, "failed")

    def test_load_or_infer_execution_status_uses_squeue_for_pending_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "run_001"
            result_dir = run_dir / "job-321"
            _write_execution_status(
                result_dir,
                ExecutionStatus(
                    state="running",
                    job_key="321",
                    slurm_job_id="321",
                    started_at="start",
                    result_dir=str(result_dir),
                ),
            )
            write_latest_result_dir(run_dir, result_dir)
            with patch("slurmforge.pipeline.status.slurm.subprocess.run") as run_mock:
                run_mock.return_value.returncode = 0
                run_mock.return_value.stdout = "PENDING\n"
                run_mock.return_value.stderr = ""
                status = load_or_infer_execution_status(run_dir)

        self.assertIsNotNone(status)
        self.assertEqual(status.state, "pending")
        self.assertEqual(status.slurm_state, "PENDING")

    def test_load_or_infer_execution_status_uses_sacct_when_job_is_gone_from_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "run_001"
            result_dir = run_dir / "job-654"
            _write_execution_status(
                result_dir,
                ExecutionStatus(
                    state="running",
                    job_key="654",
                    slurm_job_id="654",
                    started_at="start",
                    result_dir=str(result_dir),
                ),
            )
            write_latest_result_dir(run_dir, result_dir)
            with patch("slurmforge.pipeline.status.slurm.subprocess.run") as run_mock:
                queue = unittest.mock.Mock(returncode=0, stdout="", stderr="")
                acct = unittest.mock.Mock(returncode=0, stdout="PREEMPTED\n", stderr="")
                run_mock.side_effect = [queue, acct]
                status = load_or_infer_execution_status(run_dir)

        self.assertIsNotNone(status)
        self.assertEqual(status.state, "failed")
        self.assertEqual(status.failure_class, "preempted")
        self.assertEqual(status.slurm_state, "PREEMPTED")

    def test_load_or_infer_execution_status_finds_array_logs_from_ancestor_batch_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "batch"
            run_dir = batch_root / "nested" / "group_a" / "run_001"
            result_dir = run_dir / "job-123"
            array_log_dir = batch_root / "array_logs"
            array_log_dir.mkdir(parents=True, exist_ok=True)
            (array_log_dir / "slurm-123.err").write_text("Job requeued due to preemption", encoding="utf-8")
            _write_execution_status(
                result_dir,
                ExecutionStatus(
                    state="running",
                    job_key="123",
                    slurm_job_id="123",
                    started_at="start",
                    result_dir=str(result_dir),
                ),
            )
            write_latest_result_dir(run_dir, result_dir)

            status = load_or_infer_execution_status(run_dir)

        self.assertIsNotNone(status)
        self.assertEqual(status.failure_class, "preempted")
        self.assertEqual(status.state, "failed")

    def test_load_or_infer_execution_status_prefers_explicit_latest_result_pointer_over_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "run_001"
            primary_result_dir, _status = begin_execution_status(run_dir, env={"SLURM_JOB_ID": "101"})
            _write_execution_status(
                primary_result_dir,
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="train_exit_code=2",
                    job_key="101",
                    slurm_job_id="101",
                    result_dir=str(primary_result_dir),
                ),
            )
            newer_result_dir = run_dir / "job-999"
            _write_execution_status(
                newer_result_dir,
                ExecutionStatus(
                    state="success",
                    reason="done",
                    job_key="999",
                    slurm_job_id="999",
                    result_dir=str(newer_result_dir),
                ),
            )
            newer_result_dir.touch()

            status = load_or_infer_execution_status(run_dir)

        self.assertIsNotNone(status)
        self.assertEqual(status.job_key, "101")
        self.assertEqual(status.failure_class, "script_error")

    def test_read_latest_result_dir_requires_relative_pointer_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "run_001"
            pointer = run_dir / "meta" / "latest_result_dir.json"
            pointer.parent.mkdir(parents=True, exist_ok=True)
            pointer.write_text(json.dumps({"schema_version": 1}, indent=2), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "result_dir_rel"):
                read_latest_result_dir(run_dir)

    def test_read_execution_result_contract_requires_relative_internal_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "runs" / "run_001" / "job-101"
            meta_dir = result_dir / "meta"
            meta_dir.mkdir(parents=True, exist_ok=True)

            status_payload = {
                "schema_version": 1,
                "state": "failed",
                "failure_class": "script_error",
                "failed_stage": "train",
                "reason": "boom",
                "job_key": "101",
                "result_dir_rel": ".",
                "train_log": str(result_dir / "logs" / "train.log"),
            }
            attempt_payload = {
                "train_exit_code": 2,
                "eval_exit_code": 0,
                "job_key": "101",
                "result_dir_rel": ".",
                "log_dir_rel": "logs",
                "train_log": str(result_dir / "logs" / "train.log"),
            }
            (meta_dir / "execution_status.json").write_text(json.dumps(status_payload, indent=2), encoding="utf-8")
            (meta_dir / "attempt_result.json").write_text(json.dumps(attempt_payload, indent=2), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "absolute internal paths"):
                read_execution_status(meta_dir / "execution_status.json")
            with self.assertRaisesRegex(ValueError, "absolute internal paths"):
                read_attempt_result(meta_dir / "attempt_result.json")

    def test_load_or_infer_execution_status_requires_explicit_latest_result_pointer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs" / "run_001"
            result_dir = run_dir / "job-123"
            _write_execution_status(
                result_dir,
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="boom",
                    job_key="123",
                    result_dir=str(result_dir),
                ),
            )

            status = load_or_infer_execution_status(run_dir)

        self.assertIsNone(status)
