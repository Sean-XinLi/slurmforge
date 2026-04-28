from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    prepare_stage_submission,
    submit_prepared_stage_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_stage_batch_for_test,
)
import tempfile
from pathlib import Path


class SubmitReconcileTests(StageBatchSystemTestCase):
    def test_standalone_submission_writes_ledger_and_reconcile_uses_it(self) -> None:
        from tests.support.slurm import FakeSlurmClient
        from slurmforge.status.reader import read_stage_status
        from slurmforge.submission.reconcile import reconcile_batch_submission

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            prepared = prepare_stage_submission(batch)
            group_job_ids = submit_prepared_stage_batch(prepared, client=client)
            self.assertTrue(
                (Path(batch.submission_root) / "submissions" / "ledger.json").exists()
            )
            for job_id in group_job_ids.values():
                client.set_job_state(job_id, "COMPLETED")
            reconcile_batch_submission(
                Path(batch.submission_root),
                client=client,
                missing_output_grace_seconds=300,
            )
            status = read_stage_status(
                Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            )
            assert status is not None
            self.assertEqual(status.state, "running")
