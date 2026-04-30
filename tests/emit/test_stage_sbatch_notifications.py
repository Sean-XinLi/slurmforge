from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    load_stage_submit_manifest,
    write_demo_project,
    write_stage_submit_files,
)
from tests.support.internal_records import materialize_stage_batch_for_test
import tempfile
from pathlib import Path


class StageSbatchNotificationTests(StageBatchSystemTestCase):
    def test_stage_submit_files_include_batch_notification_finalizer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "notifications": {
                        "email": {
                            "enabled": True,
                            "recipients": ["you@example.com"],
                            "events": ["batch_finished"],
                            "when": "afterany",
                        }
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            write_stage_submit_files(batch)
            manifest = load_stage_submit_manifest(Path(batch.submission_root))
            notifications = manifest["notifications"]
            self.assertEqual(notifications[0]["event"], "batch_finished")
            notify_path = Path(notifications[0]["sbatch_path"])
            self.assertTrue(notify_path.exists())
            notify_text = notify_path.read_text()
            self.assertIn("[NOTIFY] event=${NOTIFICATION_EVENT}", notify_text)
            self.assertNotIn("slurmforge.notifications.finalizer_runtime", notify_text)
            submit_text = Path(manifest["submit_script"]).read_text()
            self.assertNotIn("notify_batch_finished", submit_text)
