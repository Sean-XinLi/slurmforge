from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import write_stage_batch_layout
import json
import tempfile
from pathlib import Path


class ReconcileTests(StageBatchSystemTestCase):
    def test_reconcile_grace_waits_before_missing_output_failure(self) -> None:
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            client.set_job_state("123", "COMPLETED")
            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids={"group_001": "123"},
                client=client,
                missing_output_grace_seconds=300,
            )
            status_path = (
                Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            )
            status = read_stage_status(status_path)
            assert status is not None
            self.assertEqual(status.state, "running")
            self.assertTrue((status_path / "reconcile.json").exists())
            attempt = json.loads(
                (status_path / "attempts" / "0001" / "attempt.json").read_text()
            )
            self.assertEqual(attempt["attempt_source"], "scheduler_reconcile")
            self.assertFalse(attempt["started_by_executor"])
            self.assertEqual(attempt["scheduler_state"], "COMPLETED")

    def test_reconcile_completes_executor_skeleton_attempt(self) -> None:
        from slurmforge.status.machine import commit_attempt
        from slurmforge.status.models import StageAttemptRecord
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            commit_attempt(
                run_dir,
                StageAttemptRecord(
                    attempt_id="0001",
                    stage_instance_id=batch.stage_instances[0].stage_instance_id,
                    attempt_source="executor",
                    attempt_state="running",
                    scheduler_job_id="123",
                    started_by_executor=True,
                    executor_started_at="2026-01-01T00:00:00+00:00",
                    started_at="2026-01-01T00:00:00+00:00",
                ),
            )
            client = FakeSlurmClient()
            client.set_job_state("123", "FAILED", exit_code="1:0", reason="node failed")

            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids={"group_001": "123"},
                client=client,
                missing_output_grace_seconds=0,
            )

            attempt = json.loads(
                (run_dir / "attempts" / "0001" / "attempt.json").read_text()
            )
            self.assertEqual(attempt["attempt_source"], "executor")
            self.assertEqual(attempt["attempt_state"], "reconciled")
            self.assertEqual(attempt["scheduler_state"], "FAILED")
            self.assertEqual(attempt["exit_code"], 1)
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.latest_attempt_id, "0001")
            self.assertEqual(status.state, "failed")

    def test_reconcile_missing_output_expiry_creates_scheduler_attempt_failure(
        self,
    ) -> None:
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            client.set_job_state("123", "COMPLETED", exit_code="0:0")

            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids={"group_001": "123"},
                client=client,
                missing_output_grace_seconds=0,
            )

            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "failed")
            self.assertEqual(status.failure_class, "missing_attempt_result")
            self.assertEqual(status.latest_attempt_id, "0001")
            attempt = json.loads(
                (run_dir / "attempts" / "0001" / "attempt.json").read_text()
            )
            self.assertEqual(attempt["attempt_source"], "scheduler_reconcile")
            self.assertEqual(attempt["attempt_state"], "reconciled")
            self.assertEqual(attempt["scheduler_job_id"], "123")

    def test_sacct_parser_maps_array_tasks_and_ignores_job_steps(self) -> None:
        from slurmforge.slurm import parse_sacct_rows

        states = parse_sacct_rows(
            "\n".join(
                [
                    "123|123|4294967294|4294967294|RUNNING|0:0|",
                    "123_0|123_0|123|0|COMPLETED|0:0|",
                    "123_0.batch|123.batch|123|0|COMPLETED|0:0|",
                    "123_1|123_1|123|1|FAILED|1:0|node failed",
                ]
            )
        )

        self.assertIn("123", states)
        self.assertIn("123_0", states)
        self.assertIn("123_1", states)
        self.assertNotIn("123_0.batch", states)
        self.assertEqual(states["123_0"].array_job_id, "123")
        self.assertEqual(states["123_0"].array_task_id, 0)
        self.assertEqual(states["123_1"].state, "FAILED")

    def test_reconcile_does_not_apply_array_master_state_to_every_task(self) -> None:
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.entry.args.lr": [0.001, 0.002]},
                    }
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            self.assertEqual(batch.group_plans[0].array_size, 2)
            client = FakeSlurmClient()
            client.set_job_state("123", "COMPLETED")

            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids={"group_001": "123"},
                client=client,
                missing_output_grace_seconds=0,
            )

            for instance in batch.stage_instances:
                status = read_stage_status(
                    Path(batch.submission_root) / instance.run_dir_rel
                )
                assert status is not None
                self.assertEqual(status.state, "planned")

    def test_reconcile_applies_failed_array_master_to_each_task(self) -> None:
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.entry.args.lr": [0.001, 0.002]},
                    }
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            client.set_job_state(
                "123", "CANCELLED", exit_code="0:15", reason="cancelled by user"
            )

            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids={"group_001": "123"},
                client=client,
                missing_output_grace_seconds=0,
            )

            for instance in batch.stage_instances:
                run_dir = Path(batch.submission_root) / instance.run_dir_rel
                status = read_stage_status(run_dir)
                assert status is not None
                self.assertEqual(status.state, "cancelled")
                attempt = json.loads(
                    (run_dir / "attempts" / "0001" / "attempt.json").read_text()
                )
                self.assertEqual(attempt["attempt_state"], "reconciled")
                self.assertEqual(attempt["scheduler_state"], "CANCELLED")

    def test_squeue_observation_updates_running_status_when_sacct_is_empty(
        self,
    ) -> None:
        from slurmforge.slurm import SlurmClient, SlurmJobState, parse_squeue_rows
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm

        class SqueueOnlyClient(SlurmClient):
            def query_jobs(self, job_ids):
                return {}

            def query_active_jobs(self, job_ids):
                return {
                    "123_0": SlurmJobState(
                        job_id="123_0",
                        state="RUNNING",
                        array_job_id="123",
                        array_task_id=0,
                        reason="node-a",
                    )
                }

        states = parse_squeue_rows("123_0|RUNNING|node-a\n")
        self.assertEqual(states["123_0"].array_job_id, "123")
        self.assertEqual(states["123_0"].array_task_id, 0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)

            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids={"group_001": "123"},
                client=SqueueOnlyClient(),
                missing_output_grace_seconds=0,
            )

            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "running")
            self.assertTrue(
                (Path(batch.submission_root) / "scheduler_observations.jsonl").exists()
            )
