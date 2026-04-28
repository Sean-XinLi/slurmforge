from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import (
    write_train_eval_pipeline_layout,
)
import json
import tempfile
from pathlib import Path


class ControllerSubmissionTests(StageBatchSystemTestCase):
    def test_pipeline_controller_job_is_persisted_and_reconciled(self) -> None:
        from slurmforge.orchestration.controller import (
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

    def test_failed_pipeline_controller_submit_does_not_create_submission_fact(
        self,
    ) -> None:
        from slurmforge.orchestration.controller import submit_controller_job
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
