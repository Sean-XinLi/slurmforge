from __future__ import annotations

import tempfile
from pathlib import Path

from tests.control.pipeline_helpers import read_json
from tests.control.pipeline_overlays import grid_runs
from tests.control.pipeline_overlays import with_current_python
from tests.helpers.overlays import apply_overlay
from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
    read_submission_ledger,
    write_submission_ledger,
)
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    prepare_stage_submission,
    render_pipeline_gate_sbatch,
    write_demo_project,
)


class PipelineInitialSubmitTests(StageBatchSystemTestCase):
    def test_initial_submit_records_instances_and_dispatch_submission(self) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.workflow_contract import STAGE_INSTANCE_GATE
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=with_current_python(
                        {"orchestration": {"control": {"partition": None}}}
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)

            client = FakeSlurmClient()
            result = submit_initial_pipeline(plan, client=client)
            pipeline_root = Path(plan.root_dir)
            workflow_state = read_json(pipeline_root / "control" / "workflow_state.json")
            control_submissions = read_json(
                pipeline_root / "control" / "control_submissions.json"
            )
            train_instance = plan.stage_batches["train"].stage_instances[0]
            train_dispatch_group = plan.stage_batches["train"].group_plans[0]
            train_submission = workflow_state["submissions"]["train_initial"]
            submitted_dispatch_group = train_submission["groups"][
                train_dispatch_group.group_id
            ]
            sbatch = render_pipeline_gate_sbatch(
                plan,
                STAGE_INSTANCE_GATE,
                target_id=train_instance.stage_instance_id,
            )

            self.assertEqual(result.state, "streaming")
            self.assertEqual(workflow_state["schema_version"], 2)
            self.assertEqual(set(workflow_state["submissions"]), {"train_initial"})
            self.assertEqual(train_submission["role"], "initial")
            self.assertEqual(train_submission["display_key"], "train")
            self.assertNotIn("scheduler_job_ids", train_submission)
            self.assertEqual(
                submitted_dispatch_group["scheduler_job_id"],
                client.submissions[0].job_id,
            )
            self.assertEqual(
                submitted_dispatch_group["stage_instance_gate_job_id"],
                client.submissions[1].job_id,
            )
            self.assertEqual(
                submitted_dispatch_group["task_ids_by_instance"][
                    train_instance.stage_instance_id
                ],
                "0" if train_dispatch_group.array_size > 1 else "",
            )
            self.assertEqual(control_submissions["schema_version"], 1)
            self.assertEqual(
                set(control_submissions["submissions"]),
                {
                    f"stage_instance_gate:train_initial:{train_dispatch_group.group_id}",
                    "dispatch_catchup_gate:train_initial",
                },
            )
            self.assertIn(train_instance.stage_instance_id, workflow_state["instances"])
            self.assertIn(
                plan.stage_batches["eval"].stage_instances[0].stage_instance_id,
                workflow_state["instances"],
            )
            self.assertEqual(workflow_state["dispatch_queue"], [])
            self.assertNotIn("train_groups", workflow_state)
            self.assertNotIn("#SBATCH --partition=gpu", sbatch)
            self.assertNotIn("#SBATCH --partition", sbatch)

    def test_pipeline_result_stage_jobs_do_not_require_stage_submission_ledger(
        self,
    ) -> None:
        from slurmforge.control.state import load_workflow_state
        from slurmforge.control.state_model import result_from_state
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.submission.ledger import ledger_path
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=with_current_python(
                        apply_overlay(
                            grid_runs(1),
                            {"dispatch": {"max_available_gpus": 1}},
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            ledger_path(Path(plan.stage_batches["train"].submission_root)).unlink()

            state = load_workflow_state(Path(plan.root_dir), plan)
            result = result_from_state(Path(plan.root_dir), state)

            self.assertEqual(
                result.submitted_stage_job_ids["train"],
                {"group_001": client.submissions[0].job_id},
            )

    def test_train_submission_reuses_partial_stage_submission_without_duplicate(
        self,
    ) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra=with_current_python(
                    {
                        "runs": {
                            "type": "grid",
                            "axes": {"train.resources.constraint": ["a", "b"]},
                        },
                        "dispatch": {
                            "max_available_gpus": 2,
                            "overflow_policy": "serialize_groups",
                        },
                    }
                ),
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

            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)

            stage_submissions = [
                submission.path.name
                for submission in client.submissions
                if submission.path.name.endswith(".sbatch")
            ]
            self.assertIn(f"{second_group}.sbatch", stage_submissions)
            self.assertNotIn(f"{first_group}.sbatch", stage_submissions)
