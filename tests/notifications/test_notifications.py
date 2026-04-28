from __future__ import annotations

from slurmforge.contracts import (
    NotificationRunStatusInput,
    NotificationStageStatusInput,
    NotificationSummaryInput,
)
from slurmforge.notifications import deliver_notification
from slurmforge.notifications.records import read_notification_record
from slurmforge.plans.notifications import EmailNotificationPlan, NotificationPlan
from tests.support.case import StageBatchSystemTestCase
import tempfile
from pathlib import Path
from unittest.mock import patch


class NotificationDeliveryTests(StageBatchSystemTestCase):
    def test_email_delivery_failure_writes_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            notification_plan = NotificationPlan(
                email=EmailNotificationPlan(
                    enabled=True,
                    to=("you@example.com",),
                    events=("batch_finished",),
                    sendmail="/bad/sendmail",
                ),
            )
            summary_input = NotificationSummaryInput(
                event="batch_finished",
                root_kind="stage_batch",
                root=str(root),
                project="demo",
                experiment="baseline",
                object_id="train-batch",
                state="failed",
                run_statuses=(
                    NotificationRunStatusInput(run_id="run_001", state="failed"),
                ),
                stage_statuses=(
                    NotificationStageStatusInput(
                        run_id="run_001",
                        stage_name="train",
                        state="failed",
                        failure_class="script_error",
                        reason="boom",
                    ),
                ),
            )

            with patch(
                "slurmforge.notifications.delivery.send_email_summary",
                side_effect=RuntimeError("sendmail unavailable"),
            ):
                record = deliver_notification(
                    root,
                    event="batch_finished",
                    notification_plan=notification_plan,
                    summary_input=summary_input,
                )

            assert record is not None
            self.assertEqual(record.state, "failed")
            self.assertEqual(record.reason, "sendmail unavailable")
            diagnostic = root / "notifications" / "batch_finished_email_traceback.log"
            self.assertTrue(diagnostic.exists())
            diagnostic_text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: sendmail unavailable", diagnostic_text)
            self.assertIn("Traceback", diagnostic_text)
            persisted = read_notification_record(root, "batch_finished", "email")
            assert persisted is not None
            self.assertEqual(persisted.state, "failed")
