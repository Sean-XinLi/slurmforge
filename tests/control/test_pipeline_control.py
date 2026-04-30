from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from tests.helpers.overlays import apply_overlay
from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
    read_submission_ledger,
    write_submission_ledger,
)
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    prepare_stage_submission,
    render_pipeline_gate_sbatch,
    write_demo_project,
)


def _with_current_python(extra: dict | None = None) -> dict:
    runtime = {
        "runtime": {
            "executor": {"python": {"bin": sys.executable}},
            "user": {"default": {"python": {"bin": sys.executable}}},
        }
    }
    return apply_overlay(runtime, extra or {})


def _grid_runs(count: int) -> dict:
    return {
        "runs": {
            "type": "grid",
            "axes": {"train.entry.args.lr": [0.001 + index for index in range(count)]},
        }
    }


class PipelineControlTests(StageBatchSystemTestCase):
    def test_initial_submit_records_instances_and_dispatch_submission(self) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.workflow_contract import STAGE_INSTANCE_GATE
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        {"orchestration": {"control": {"partition": None}}}
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)

            result = submit_initial_pipeline(plan, client=FakeSlurmClient())
            pipeline_root = Path(plan.root_dir)
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            train_instance = plan.stage_batches["train"].stage_instances[0]
            sbatch = render_pipeline_gate_sbatch(
                plan,
                STAGE_INSTANCE_GATE,
                stage_instance_id=train_instance.stage_instance_id,
            )

            self.assertEqual(result.state, "streaming")
            self.assertEqual(set(workflow_state["submissions"]), {"train_initial"})
            self.assertIn(train_instance.stage_instance_id, workflow_state["instances"])
            self.assertIn(
                plan.stage_batches["eval"].stage_instances[0].stage_instance_id,
                workflow_state["instances"],
            )
            self.assertEqual(workflow_state["dispatch_queue"], [])
            self.assertNotIn("train_groups", workflow_state)
            self.assertNotIn("#SBATCH --partition=gpu", sbatch)
            self.assertNotIn("#SBATCH --partition", sbatch)

    def test_per_run_release_dispatches_one_eval_while_other_train_tasks_run(
        self,
    ) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        apply_overlay(
                            _grid_runs(4),
                            {"dispatch": {"max_available_gpus": 4}},
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")
            for task_index in (1, 2, 3):
                client.set_array_task_state(train_job_id, task_index, "RUNNING")

            first_train = plan.stage_batches["train"].stage_instances[0]
            first_eval = plan.stage_batches["eval"].stage_instances[0]
            result = advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=first_train.stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )
            advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=first_train.stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )

            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            self.assertEqual(result.state, "streaming")
            self.assertEqual(len(client.submissions), 6)
            self.assertEqual(
                workflow_state["instances"][first_eval.stage_instance_id]["state"],
                "submitted",
            )
            for eval_instance in plan.stage_batches["eval"].stage_instances[1:]:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "planned",
                )

    def test_failed_train_instance_blocks_matching_eval_only(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        apply_overlay(
                            _grid_runs(2),
                            {"dispatch": {"max_available_gpus": 2}},
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")
            client.set_array_task_state(train_job_id, 1, "FAILED")

            advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=plan.stage_batches["train"]
                .stage_instances[0]
                .stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )

            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            eval_instances = plan.stage_batches["eval"].stage_instances
            self.assertEqual(
                workflow_state["instances"][eval_instances[0].stage_instance_id][
                    "state"
                ],
                "submitted",
            )
            self.assertEqual(
                workflow_state["instances"][eval_instances[1].stage_instance_id][
                    "state"
                ],
                "blocked",
            )

    def test_per_group_release_waits_for_whole_train_group(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        apply_overlay(
                            _grid_runs(2),
                            {
                                "dispatch": {
                                    "max_available_gpus": 2,
                                    "release_policy": "per_group",
                                }
                            },
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")
            client.set_array_task_state(train_job_id, 1, "RUNNING")

            advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=plan.stage_batches["train"]
                .stage_instances[0]
                .stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            for eval_instance in plan.stage_batches["eval"].stage_instances:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "planned",
                )

            self.assertEqual(execute_stage_task(train_root, 1, 1), 0)
            client.set_array_task_state(train_job_id, 1, "COMPLETED")
            advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=plan.stage_batches["train"]
                .stage_instances[1]
                .stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            for eval_instance in plan.stage_batches["eval"].stage_instances:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "submitted",
                )

    def test_windowed_release_batches_ready_eval_instances(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        apply_overlay(
                            _grid_runs(2),
                            {
                                "dispatch": {
                                    "max_available_gpus": 2,
                                    "release_policy": "windowed",
                                    "window_size": 2,
                                    "window_seconds": 999,
                                }
                            },
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")
            client.set_array_task_state(train_job_id, 1, "RUNNING")

            advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=plan.stage_batches["train"]
                .stage_instances[0]
                .stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            self.assertEqual(
                workflow_state["instances"][
                    plan.stage_batches["eval"].stage_instances[0].stage_instance_id
                ]["state"],
                "ready",
            )

            self.assertEqual(execute_stage_task(train_root, 1, 1), 0)
            client.set_array_task_state(train_job_id, 1, "COMPLETED")
            advance_pipeline_once(
                Path(plan.root_dir),
                event="stage-instance-finished",
                stage_instance_id=plan.stage_batches["train"]
                .stage_instances[1]
                .stage_instance_id,
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            for eval_instance in plan.stage_batches["eval"].stage_instances:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "submitted",
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
                extra=_with_current_python(
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
