from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import materialize_stage_batch_for_test
import json
import tempfile
from pathlib import Path


class ReconcileMissingOutputTests(StageBatchSystemTestCase):
    def test_reconcile_grace_waits_before_missing_output_failure(self) -> None:
        from slurmforge.status.reader import read_stage_status
        from slurmforge.status.reconcile import reconcile_stage_batch_with_slurm
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
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
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
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
