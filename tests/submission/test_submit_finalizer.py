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


class SubmitFinalizerTests(StageBatchSystemTestCase):
    def test_batch_notification_finalizer_submits_after_terminal_groups(self) -> None:
        from slurmforge.notifications.records import read_notification_record
        from slurmforge.submission.finalizer import (
            finalizer_dependency_group_ids,
            submit_stage_batch_finalizer,
        )
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "notifications": {
                        "email": {
                            "enabled": True,
                            "to": ["you@example.com"],
                            "on": ["batch_finished"],
                            "mode": "summary",
                        }
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            self.assertEqual(finalizer_dependency_group_ids(batch), ("group_001",))
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            prepared = prepare_stage_submission(batch)
            client = FakeSlurmClient()
            group_job_ids = submit_prepared_stage_batch(prepared, client=client)

            record = submit_stage_batch_finalizer(batch, group_job_ids, client=client)

            assert record is not None
            self.assertEqual(record.state, "submitted")
            self.assertEqual(record.scheduler_job_id, "1002")
            self.assertEqual(
                client.submissions[-1][0].name, "notify_batch_finished.sbatch"
            )
            self.assertEqual(
                client.submissions[-1][1], f"afterany:{group_job_ids['group_001']}"
            )
            persisted = read_notification_record(
                Path(batch.submission_root), "batch_finished"
            )
            assert persisted is not None
            self.assertEqual(persisted.scheduler_job_id, record.scheduler_job_id)
