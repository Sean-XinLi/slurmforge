from __future__ import annotations

import json
from pathlib import Path
import tempfile

from slurmforge.errors import RecordContractError
from slurmforge.notifications.models import NotificationSubmissionRecord
from slurmforge.notifications.records import read_notification_record
from slurmforge.notifications.records import write_notification_record
from slurmforge.plans.notifications import EmailNotificationPlan, NotificationPlan
from slurmforge.submission.notification_mail import submit_slurm_mail_notification
from tests.support.case import StageBatchSystemTestCase
from tests.support.slurm import FakeSlurmClient


class NotificationSubmissionTests(StageBatchSystemTestCase):
    def test_slurm_mail_can_submit_without_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            notify_path = root / "notify.sbatch"
            notify_path.write_text("#!/usr/bin/env bash\ntrue\n", encoding="utf-8")
            notification_plan = NotificationPlan(
                email=EmailNotificationPlan(
                    enabled=True,
                    recipients=("you@example.com",),
                    events=("batch_finished",),
                    when="afterany",
                ),
            )
            client = FakeSlurmClient()

            record = submit_slurm_mail_notification(
                root=root,
                root_kind="stage_batch",
                event="batch_finished",
                notification_plan=notification_plan,
                dependency_job_ids=(),
                sbatch_path=notify_path,
                client=client,
                barrier_path_factory=lambda index: root / f"barrier_{index}.sbatch",
            )

            assert record is not None
            self.assertEqual(record.state, "submitted")
            self.assertEqual(record.dependency_job_ids, ())
            self.assertEqual(record.barrier_job_ids, ())
            self.assertEqual(client.submissions[0].options.dependency, "")

    def test_slurm_mail_submit_failure_writes_diagnostic(self) -> None:
        class FailingSlurm(FakeSlurmClient):
            def submit(self, path, *, options=None):
                raise RuntimeError("sbatch unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            notify_path = root / "notify.sbatch"
            notify_path.write_text("#!/usr/bin/env bash\ntrue\n", encoding="utf-8")
            notification_plan = NotificationPlan(
                email=EmailNotificationPlan(
                    enabled=True,
                    recipients=("you@example.com",),
                    events=("batch_finished",),
                    when="afterany",
                ),
            )

            record = submit_slurm_mail_notification(
                root=root,
                root_kind="stage_batch",
                event="batch_finished",
                notification_plan=notification_plan,
                dependency_job_ids=("1001",),
                sbatch_path=notify_path,
                client=FailingSlurm(),
                barrier_path_factory=lambda index: root / f"barrier_{index}.sbatch",
            )

            assert record is not None
            self.assertEqual(record.state, "failed")
            self.assertEqual(record.reason, "sbatch unavailable")
            diagnostic = (
                root
                / "notifications"
                / "batch_finished_slurm_mail_submit_traceback.log"
            )
            self.assertTrue(diagnostic.exists())
            diagnostic_text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: sbatch unavailable", diagnostic_text)
            self.assertIn("Traceback", diagnostic_text)
            persisted = read_notification_record(root, "batch_finished")
            assert persisted is not None
            self.assertEqual(persisted.state, "failed")

    def test_malformed_notification_record_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = {
                "event": "batch_finished",
                "root_kind": "stage_batch",
                "root": str(root),
                "backend": "slurm_mail",
                "recipients": ("you@example.com",),
                "sbatch_paths": ("notify.sbatch",),
                "dependency_type": "afterany",
                "mail_type": "END",
            }

            with self.assertRaises(RecordContractError):
                write_notification_record(
                    root,
                    NotificationSubmissionRecord(
                        **base,
                        state="submitted",
                        scheduler_job_ids=(),
                    ),
                )

            with self.assertRaises(RecordContractError):
                write_notification_record(
                    root,
                    NotificationSubmissionRecord(
                        **base,
                        state="failed",
                        scheduler_job_ids=(),
                        reason="",
                    ),
                )

            for field_name in (
                "scheduler_job_ids",
                "sbatch_paths",
                "barrier_job_ids",
                "dependency_job_ids",
            ):
                with self.subTest(field_name=field_name):
                    record_kwargs = {
                        **base,
                        "state": "submitted",
                        "scheduler_job_ids": ("1001",),
                    }
                    record_kwargs[field_name] = ("",)
                    with self.assertRaises(RecordContractError):
                        write_notification_record(
                            root, NotificationSubmissionRecord(**record_kwargs)
                        )

            invalid_item_cases = {
                "list_recipients_field": {"recipients": ["you@example.com"]},
                "list_scheduler_field": {"scheduler_job_ids": ["1001"]},
                "path_sbatch_item": {"sbatch_paths": (Path("notify.sbatch"),)},
                "integer_scheduler_item": {"scheduler_job_ids": (1001,)},
                "integer_barrier_item": {"barrier_job_ids": (1002,)},
                "integer_dependency_item": {"dependency_job_ids": (1003,)},
            }
            for name, override in invalid_item_cases.items():
                with self.subTest(name=name):
                    record_kwargs = {
                        **base,
                        "state": "submitted",
                        "scheduler_job_ids": ("1001",),
                        **override,
                    }
                    with self.assertRaises(RecordContractError):
                        write_notification_record(
                            root, NotificationSubmissionRecord(**record_kwargs)
                        )

    def test_malformed_notification_record_payload_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            record_dir = root / "notifications" / "records"
            record_dir.mkdir(parents=True)
            payload = {
                "schema_version": 1,
                "event": "batch_finished",
                "root_kind": "stage_batch",
                "root": str(root),
                "backend": "slurm_mail",
                "state": "submitted",
                "recipients": ["you@example.com"],
                "scheduler_job_ids": ["1001"],
                "sbatch_paths": ["notify.sbatch"],
                "barrier_job_ids": [],
                "dependency_job_ids": [],
                "dependency_type": "afterany",
                "mail_type": "END",
                "submitted_at": "2026-01-01T00:00:00Z",
                "reason": "",
            }

            for field_name in (
                "scheduler_job_ids",
                "sbatch_paths",
                "barrier_job_ids",
                "dependency_job_ids",
            ):
                with self.subTest(field_name=field_name):
                    record_payload = {**payload, field_name: [""]}
                    (record_dir / "batch_finished.slurm_mail.json").write_text(
                        json.dumps(record_payload), encoding="utf-8"
                    )
                    with self.assertRaises(RecordContractError):
                        read_notification_record(root, "batch_finished")
