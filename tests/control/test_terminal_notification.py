from __future__ import annotations

import tempfile

from tests.control.pipeline_helpers import read_json
from tests.control.pipeline_overlays import terminal_email_overlay
from tests.control.pipeline_scenarios import advance_eval_completion
from tests.control.pipeline_scenarios import advance_train_completion
from tests.control.pipeline_scenarios import build_train_eval_control_scenario
from tests.control.pipeline_scenarios import complete_eval_from_workflow_state
from tests.control.pipeline_scenarios import submit_initial_and_complete_train_task
from tests.support.case import StageBatchSystemTestCase


class TerminalNotificationTests(StageBatchSystemTestCase):
    def test_pipeline_terminal_notification_submits_once(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.notifications.records import read_notification_record

        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=terminal_email_overlay(["you@example.com", "ops@example.com"]),
            )
            submit_initial_and_complete_train_task(scenario)
            advance_train_completion(scenario)
            complete_eval_from_workflow_state(scenario)

            result = advance_eval_completion(scenario)
            advance_pipeline_once(
                scenario.pipeline_root,
                client=scenario.client,
                missing_output_grace_seconds=0,
            )

            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            control_submissions = read_json(
                scenario.pipeline_root / "control" / "control_submissions.json"
            )
            notify_submissions = [
                submission
                for submission in scenario.client.submissions
                if submission.path.name == "notify_train_eval_pipeline_finished.sbatch"
            ]
            self.assertEqual(len(notify_submissions), 2)
            self.assertEqual(notify_submissions[0].options.mail_user, "you@example.com")
            self.assertEqual(notify_submissions[1].options.mail_user, "ops@example.com")
            self.assertEqual(notify_submissions[0].options.mail_type, "END")
            record = read_notification_record(
                scenario.pipeline_root, "train_eval_pipeline_finished"
            )
            assert record is not None
            notification_job_ids = tuple(
                submission.job_id for submission in notify_submissions
            )
            self.assertEqual(record.scheduler_job_ids, notification_job_ids)
            terminal_aggregation = workflow_state["terminal_aggregation"]
            notification_key = "terminal_notification:train_eval_pipeline_finished"
            self.assertEqual(terminal_aggregation["workflow_terminal_state"], "success")
            self.assertEqual(terminal_aggregation["state"], "submitted")
            self.assertEqual(
                terminal_aggregation["notification_control_key"],
                notification_key,
            )
            self.assertNotIn("notification_job_ids", terminal_aggregation)
            self.assertIn(notification_key, control_submissions["submissions"])
            self.assertEqual(
                control_submissions["submissions"][notification_key][
                    "scheduler_job_ids"
                ],
                list(notification_job_ids),
            )
            self.assertEqual(
                result.submitted_control_job_ids[notification_key],
                notification_job_ids,
            )
            terminal_eval_submission = next(
                item
                for item in workflow_state["submissions"].values()
                if item["stage_name"] == "eval"
            )
            self.assertEqual(terminal_eval_submission["state"], "terminal")
            self.assertEqual(
                next(iter(terminal_eval_submission["groups"].values()))["state"],
                "terminal",
            )

    def test_terminal_notification_failed_state_can_recover_on_next_advance(
        self,
    ) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.notifications.records import read_notification_record
        from tests.support.slurm import FakeSlurmClient
        from tests.support.workflow_records import read_workflow_status_record

        class FailingNotificationSlurm(FakeSlurmClient):
            def submit(self, path, *, options=None):
                if path.name == "notify_train_eval_pipeline_finished.sbatch":
                    raise RuntimeError("mail scheduler unavailable")
                return super().submit(path, options=options)

        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=terminal_email_overlay(["you@example.com"]),
                client=FailingNotificationSlurm(),
            )
            submit_initial_and_complete_train_task(scenario)
            advance_train_completion(scenario)
            complete_eval_from_workflow_state(scenario)

            advance_eval_completion(scenario)
            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            workflow_status = read_workflow_status_record(scenario.pipeline_root)
            notification_key = "terminal_notification:train_eval_pipeline_finished"
            self.assertEqual(workflow_state["terminal_aggregation"]["state"], "failed")
            self.assertEqual(
                workflow_status.control_jobs[notification_key].state,
                "failed",
            )
            self.assertIn(
                "mail scheduler unavailable",
                workflow_status.control_jobs[notification_key].reason,
            )

            recovery_client = FakeSlurmClient()
            advance_pipeline_once(scenario.pipeline_root, client=recovery_client)

            record = read_notification_record(
                scenario.pipeline_root, "train_eval_pipeline_finished"
            )
            assert record is not None
            self.assertEqual(record.state, "submitted")
            self.assertEqual(record.scheduler_job_ids, ("1001",))

    def test_terminal_notification_partial_submit_is_uncertain_and_not_retried(
        self,
    ) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from tests.support.slurm import FakeSlurmClient
        from tests.support.workflow_records import read_workflow_status_record

        class PartiallyFailingNotificationSlurm(FakeSlurmClient):
            def submit(self, path, *, options=None):
                if (
                    path.name == "notify_train_eval_pipeline_finished.sbatch"
                    and options is not None
                    and options.mail_user == "ops@example.com"
                ):
                    raise RuntimeError("second recipient failed")
                return super().submit(path, options=options)

        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=terminal_email_overlay(["you@example.com", "ops@example.com"]),
                client=PartiallyFailingNotificationSlurm(),
            )
            submit_initial_and_complete_train_task(scenario)
            advance_train_completion(scenario)
            complete_eval_from_workflow_state(scenario)

            advance_eval_completion(scenario)
            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            control_submissions = read_json(
                scenario.pipeline_root / "control" / "control_submissions.json"
            )
            workflow_status = read_workflow_status_record(scenario.pipeline_root)
            notification_key = "terminal_notification:train_eval_pipeline_finished"
            self.assertEqual(
                workflow_state["terminal_aggregation"]["state"],
                "uncertain",
            )
            self.assertEqual(
                control_submissions["submissions"][notification_key]["state"],
                "uncertain",
            )
            self.assertEqual(
                len(
                    control_submissions["submissions"][notification_key][
                        "scheduler_job_ids"
                    ]
                ),
                1,
            )
            self.assertEqual(
                workflow_status.control_jobs[notification_key].state,
                "uncertain",
            )
            self.assertEqual(
                len(workflow_status.control_jobs[notification_key].scheduler_job_ids),
                1,
            )
            submissions_before = len(scenario.client.submissions)
            advance_pipeline_once(scenario.pipeline_root, client=scenario.client)
            self.assertEqual(len(scenario.client.submissions), submissions_before)
