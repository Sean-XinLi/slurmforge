from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import materialize_stage_batch_for_test
import tempfile
from pathlib import Path


class ReconcileSqueueTests(StageBatchSystemTestCase):
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
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)

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
