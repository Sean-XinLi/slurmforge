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


class ReconcileArrayJobTests(StageBatchSystemTestCase):
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
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
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
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
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
