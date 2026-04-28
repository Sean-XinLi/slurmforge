from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    SchemaVersion,
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    prepare_stage_submission,
    write_demo_project,
)
from tests.support.internal_records import (
    read_submission_ledger,
    write_train_eval_pipeline_layout,
    write_submission_ledger,
)
import json
import tempfile
from pathlib import Path
from unittest.mock import patch


class ControllerTests(StageBatchSystemTestCase):
    def test_pipeline_controller_job_is_persisted_and_reconciled(self) -> None:
        from slurmforge.orchestration import (
            reconcile_controller_job,
            submit_controller_job,
        )
        from tests.support.slurm import FakeSlurmClient
        from slurmforge.storage.controller import (
            read_controller_job,
            read_controller_status,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()

            record = submit_controller_job(plan, client=client)

            pipeline_root = Path(plan.root_dir)
            job_path = pipeline_root / "controller" / "controller_job.json"
            self.assertTrue(job_path.exists())
            self.assertEqual(record.scheduler_job_id, "1001")
            self.assertTrue(Path(record.sbatch_path).is_absolute())
            persisted = read_controller_job(pipeline_root)
            assert persisted is not None
            self.assertEqual(persisted.scheduler_job_id, record.scheduler_job_id)
            job_payload = json.loads(job_path.read_text(encoding="utf-8"))
            self.assertEqual(
                set(job_payload),
                {
                    "schema_version",
                    "pipeline_id",
                    "scheduler_job_id",
                    "submitted_at",
                    "sbatch_path",
                },
            )
            immutable_job = job_path.read_text(encoding="utf-8")
            status = read_controller_status(pipeline_root)
            assert status is not None
            self.assertEqual(status["state"], "queued")
            self.assertEqual(status["scheduler_job_id"], record.scheduler_job_id)

            client.set_job_state(record.scheduler_job_id, "RUNNING")
            running = reconcile_controller_job(pipeline_root, client=client)
            assert running is not None
            self.assertFalse(hasattr(running, "state"))
            self.assertEqual(job_path.read_text(encoding="utf-8"), immutable_job)
            status = read_controller_status(pipeline_root)
            assert status is not None
            self.assertEqual(status["state"], "running")
            self.assertEqual(status["scheduler_state"], "RUNNING")

            client.set_job_state(record.scheduler_job_id, "COMPLETED", exit_code="0:0")
            completed = reconcile_controller_job(pipeline_root, client=client)
            assert completed is not None
            self.assertFalse(hasattr(completed, "state"))
            self.assertEqual(job_path.read_text(encoding="utf-8"), immutable_job)
            status = read_controller_status(pipeline_root)
            assert status is not None
            self.assertEqual(status["state"], "success")
            self.assertEqual(status["scheduler_job_id"], record.scheduler_job_id)

    def test_controller_unknown_error_writes_traceback_log(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
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

    def test_failed_pipeline_controller_submit_does_not_create_submission_fact(
        self,
    ) -> None:
        from slurmforge.orchestration import submit_controller_job
        from tests.support.slurm import FakeSlurmClient
        from slurmforge.storage.controller import read_controller_status

        class FailingSlurm(FakeSlurmClient):
            def submit(self, path, *, dependency=None):
                raise RuntimeError("sbatch unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)

            with self.assertRaisesRegex(RuntimeError, "sbatch unavailable"):
                submit_controller_job(plan, client=FailingSlurm())

            self.assertFalse(
                (pipeline_root / "controller" / "controller_job.json").exists()
            )
            diagnostic = (
                pipeline_root / "controller" / "controller_submit_traceback.log"
            )
            self.assertTrue(diagnostic.exists())
            diagnostic_text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: sbatch unavailable", diagnostic_text)
            self.assertIn("Traceback", diagnostic_text)
            status = read_controller_status(pipeline_root)
            assert status is not None
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["reason"], "sbatch unavailable")

    def test_controller_persists_state_and_blocked_pipeline_is_not_success(
        self,
    ) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from slurmforge.orchestration import submit_controller_job
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            (root / "train.py").write_text("raise SystemExit(1)\n", encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
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
        from slurmforge.status import StageStatusRecord, commit_stage_status

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
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
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

    def test_controller_resume_does_not_duplicate_submitted_stage(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from tests.support.slurm import FakeSlurmClient

        class CompletingFakeSlurm(FakeSlurmClient):
            def submit(self, path, *, dependency=None):
                job_id = super().submit(path, dependency=dependency)
                self.set_job_state(job_id, "COMPLETED")
                return job_id

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            client = CompletingFakeSlurm()

            self.assertEqual(
                run_controller(
                    Path(plan.root_dir),
                    client=client,
                    poll_seconds=0,
                    missing_output_grace_seconds=0,
                ),
                1,
            )
            self.assertEqual(len(client.submissions), 1)
            self.assertEqual(
                run_controller(
                    Path(plan.root_dir),
                    client=client,
                    poll_seconds=0,
                    missing_output_grace_seconds=0,
                ),
                1,
            )
            self.assertEqual(len(client.submissions), 1)

    def test_controller_reads_submission_state_through_public_api(self) -> None:
        source = Path("src/slurmforge/controller/stage_runtime.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("submission._ledger", source)
        self.assertNotIn("from ..submission._ledger", source)
        self.assertNotIn("from ..submission.ledger", source)
        self.assertNotIn("submitted_group_job_ids(", source)
        self.assertIn("read_submission_state", source)

    def test_controller_stops_on_uncertain_submission_ledger(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_batch = plan.stage_batches["train"]
            prepare_stage_submission(train_batch)
            ledger = read_submission_ledger(Path(train_batch.submission_root))
            assert ledger is not None
            ledger.state = "submitting"
            ledger.groups["group_001"].state = "submitting"
            write_submission_ledger(Path(train_batch.submission_root), ledger)
            client = FakeSlurmClient()
            self.assertEqual(
                run_controller(Path(plan.root_dir), client=client, poll_seconds=0), 1
            )
            self.assertEqual(len(client.submissions), 0)

    def test_controller_recovers_partial_group_submission_without_duplicate(
        self,
    ) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from tests.support.slurm import FakeSlurmClient

        class CompletingFakeSlurm(FakeSlurmClient):
            def submit(self, path, *, dependency=None):
                job_id = super().submit(path, dependency=dependency)
                self.set_job_state(job_id, "COMPLETED")
                return job_id

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.resources.constraint": ["a", "b"]},
                    },
                    "dispatch": {
                        "max_available_gpus": 2,
                        "overflow_policy": "serialize_groups",
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_batch = plan.stage_batches["train"]
            first_group = train_batch.group_plans[0].group_id
            second_group = train_batch.group_plans[1].group_id
            prepare_stage_submission(train_batch)
            ledger = read_submission_ledger(Path(train_batch.submission_root))
            assert ledger is not None
            ledger.state = "partial"
            ledger.groups[first_group].state = "submitted"
            ledger.groups[first_group].scheduler_job_id = "111"
            write_submission_ledger(Path(train_batch.submission_root), ledger)
            self.assertEqual(
                read_submission_ledger(Path(train_batch.submission_root))
                .groups[first_group]
                .scheduler_job_id,
                "111",
            )
            client = CompletingFakeSlurm()
            client.set_job_state("111", "COMPLETED")

            self.assertEqual(
                run_controller(
                    Path(plan.root_dir),
                    client=client,
                    poll_seconds=0,
                    missing_output_grace_seconds=0,
                ),
                1,
            )

            self.assertEqual(len(client.submissions), 1)
            self.assertEqual(client.submissions[0][0].name, f"{second_group}.sbatch")
