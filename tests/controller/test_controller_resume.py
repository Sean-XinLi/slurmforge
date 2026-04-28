from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    prepare_stage_submission,
    write_demo_project,
)
from tests.support.internal_records import (
    read_submission_ledger,
    materialize_train_eval_pipeline_for_test,
    write_submission_ledger,
)
import tempfile
from pathlib import Path


class ControllerResumeTests(StageBatchSystemTestCase):
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
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
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

    def test_controller_stops_on_uncertain_submission_ledger(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
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
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
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
