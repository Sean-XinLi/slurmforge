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


class ReconcileAttemptTests(StageBatchSystemTestCase):
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
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
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
