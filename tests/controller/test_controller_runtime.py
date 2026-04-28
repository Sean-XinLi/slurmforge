from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    SchemaVersion,
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
)
import json
import tempfile
from pathlib import Path
from unittest.mock import patch


class ControllerRuntimeTests(StageBatchSystemTestCase):
    def test_controller_unknown_error_writes_traceback_log(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)

            with patch(
                "slurmforge.controller.train_eval_pipeline.load_experiment_spec_from_snapshot",
                side_effect=RuntimeError("controller boom"),
            ):
                self.assertEqual(run_controller(pipeline_root), 1)

            diagnostic = pipeline_root / "controller_traceback.log"
            self.assertTrue(diagnostic.exists())
            text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: controller boom", text)
            self.assertIn("Traceback", text)

    def test_controller_persists_state_and_blocked_pipeline_is_not_success(
        self,
    ) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from slurmforge.orchestration.controller import submit_controller_job
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            (root / "train.py").write_text("raise SystemExit(1)\n", encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            controller_record = submit_controller_job(plan, client=FakeSlurmClient())
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertNotEqual(execute_stage_task(train_root, 1, 0), 0)

            exit_code = run_controller(
                Path(plan.root_dir), client=FakeSlurmClient(), poll_seconds=0
            )
            self.assertEqual(exit_code, 1)
            controller_state = json.loads(
                (
                    Path(plan.root_dir) / "controller" / "controller_state.json"
                ).read_text()
            )
            self.assertEqual(controller_state["state"], "failed")
            self.assertNotIn("submitted_batches", controller_state)
            controller_status = json.loads(
                (
                    Path(plan.root_dir) / "controller" / "controller_status.json"
                ).read_text()
            )
            self.assertEqual(controller_status["state"], "failed")
            self.assertEqual(
                controller_status["scheduler_job_id"],
                controller_record.scheduler_job_id,
            )
            eval_statuses = list(
                (Path(plan.stage_batches["eval"].submission_root) / "runs").glob(
                    "*/status.json"
                )
            )
            self.assertEqual(len(eval_statuses), 1)
            self.assertEqual(
                json.loads(eval_statuses[0].read_text())["state"], "blocked"
            )

    def test_controller_sends_one_pipeline_terminal_notification(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from slurmforge.notifications.records import read_notification_record
        from tests.support.slurm import FakeSlurmClient
        from slurmforge.status.machine import commit_stage_status
        from slurmforge.status.models import StageStatusRecord

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "notifications": {
                        "email": {
                            "enabled": True,
                            "to": ["you@example.com"],
                            "on": ["train_eval_pipeline_finished"],
                            "mode": "summary",
                        }
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            for batch in plan.stage_batches.values():
                batch_root = Path(batch.submission_root)
                for instance in batch.stage_instances:
                    commit_stage_status(
                        batch_root / instance.run_dir_rel,
                        StageStatusRecord(
                            schema_version=SchemaVersion.STATUS,
                            stage_instance_id=instance.stage_instance_id,
                            run_id=instance.run_id,
                            stage_name=instance.stage_name,
                            state="success",
                        ),
                        source="test",
                    )

            sent: list[dict] = []

            def fake_send(**kwargs):
                sent.append(kwargs)

            with patch(
                "slurmforge.notifications.delivery.send_email_summary",
                side_effect=fake_send,
            ):
                self.assertEqual(
                    run_controller(
                        pipeline_root, client=FakeSlurmClient(), poll_seconds=0
                    ),
                    0,
                )
                self.assertEqual(
                    run_controller(
                        pipeline_root, client=FakeSlurmClient(), poll_seconds=0
                    ),
                    0,
                )

            self.assertEqual(len(sent), 1)
            self.assertIn("SlurmForge train/eval pipeline finished", sent[0]["body"])
            record = read_notification_record(
                pipeline_root, "train_eval_pipeline_finished"
            )
            assert record is not None
            self.assertEqual(record.state, "sent")
            self.assertEqual(record.recipients, ("you@example.com",))
