from __future__ import annotations

import json
import shutil
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


def _stage_finished(stage_instance_id: str):
    from slurmforge.control.workflow import AdvanceHint

    return AdvanceHint(
        event="stage-instance-finished",
        stage_instance_id=stage_instance_id,
    )


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

            client = FakeSlurmClient()
            result = submit_initial_pipeline(plan, client=client)
            pipeline_root = Path(plan.root_dir)
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            control_submissions = json.loads(
                (pipeline_root / "control" / "control_submissions.json").read_text()
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
                hint=_stage_finished(first_train.stage_instance_id),
                client=client,
                missing_output_grace_seconds=0,
            )
            advance_pipeline_once(
                Path(plan.root_dir),
                hint=_stage_finished(first_train.stage_instance_id),
                client=client,
                missing_output_grace_seconds=0,
            )

            workflow_state = json.loads(
                (Path(plan.root_dir) / "control" / "workflow_state.json").read_text()
            )
            events = [
                json.loads(line)
                for line in (
                    Path(plan.root_dir) / "control" / "events.jsonl"
                ).read_text(encoding="utf-8").splitlines()
            ]
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
            self.assertTrue(
                any(
                    event["event"] == "controller_advance_started"
                    and event["hint_stage_instance_id"]
                    == first_train.stage_instance_id
                    for event in events
                )
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
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[0].stage_instance_id
                ),
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
            train_submission = workflow_state["submissions"]["train_initial"]
            train_group_state = next(iter(train_submission["groups"].values()))["state"]
            self.assertEqual(train_group_state, "failed")
            self.assertEqual(train_submission["state"], "failed")

    def test_per_group_release_waits_for_whole_dispatch_group(self) -> None:
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
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[0].stage_instance_id
                ),
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
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[1].stage_instance_id
                ),
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
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[0].stage_instance_id
                ),
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
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[1].stage_instance_id
                ),
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

    def test_active_gpu_budget_uses_array_throttle_not_submitted_task_count(
        self,
    ) -> None:
        from slurmforge.control.dispatch_budget import active_budgeted_gpus
        from slurmforge.control.state import load_workflow_state
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
                            {"dispatch": {"max_available_gpus": 2}},
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            submit_initial_pipeline(plan, client=FakeSlurmClient())

            state = load_workflow_state(Path(plan.root_dir), plan)
            shutil.rmtree(Path(state.submissions["train_initial"].root))
            self.assertEqual(active_budgeted_gpus(state), 2)

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
                    extra=_with_current_python(
                        apply_overlay(
                            _grid_runs(1),
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

    def test_malformed_control_submission_ledger_is_rejected(self) -> None:
        from slurmforge.control.control_submission_ledger import (
            read_control_submission_ledger,
        )
        from slurmforge.errors import RecordContractError

        with tempfile.TemporaryDirectory() as tmp:
            pipeline_root = Path(tmp)
            control_dir = pipeline_root / "control"
            control_dir.mkdir()
            ledger_path = control_dir / "control_submissions.json"

            valid_record = {
                "key": "dispatch_catchup_gate:target",
                "kind": "dispatch_catchup_gate",
                "target_kind": "dispatch",
                "target_id": "target",
                "state": "submitted",
                "sbatch_paths": ["gate.sbatch"],
                "scheduler_job_ids": ["1001"],
            }
            cases = {
                "submissions_not_object": {
                    "schema_version": 1,
                    "submissions": [],
                },
                "payload_key_mismatch": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "key": "dispatch_catchup_gate:other",
                        }
                    },
                },
                "invalid_kind": {
                    "schema_version": 1,
                    "submissions": {
                        "bad:target": {
                            **valid_record,
                            "key": "bad:target",
                            "kind": "bad",
                        }
                    },
                },
                "invalid_state": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "state": "done",
                        }
                    },
                },
                "submitted_without_scheduler_ids": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "scheduler_job_ids": [],
                        }
                    },
                },
                "failed_without_reason": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "state": "failed",
                            "scheduler_job_ids": [],
                            "reason": "",
                        }
                    },
                },
                "empty_sbatch_paths": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "sbatch_paths": [],
                        }
                    },
                },
                "expected_key_mismatch": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:wrong": {
                            **valid_record,
                            "key": "dispatch_catchup_gate:wrong",
                        }
                    },
                },
                "scheduler_ids_not_array": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "scheduler_job_ids": "1001",
                        }
                    },
                },
            }

            for name, payload in cases.items():
                with self.subTest(name=name):
                    ledger_path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(RecordContractError):
                        read_control_submission_ledger(pipeline_root)

    def test_controller_advance_failure_records_terminal_event(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        class FailingObservedQuerySlurm(FakeSlurmClient):
            def __init__(self, source: FakeSlurmClient) -> None:
                super().__init__()
                self._next_job_id = source._next_job_id
                self.submissions = list(source.submissions)
                self.job_states = dict(source.job_states)

            def query_observed_jobs(self, job_ids):
                raise RuntimeError("scheduler query unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root, extra=_with_current_python()))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)

            with self.assertRaises(RuntimeError):
                advance_pipeline_once(
                    pipeline_root,
                    client=FailingObservedQuerySlurm(client),
                    missing_output_grace_seconds=0,
                )

            events = [
                json.loads(line)
                for line in (pipeline_root / "control" / "events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertTrue(
                any(
                    event["event"] == "controller_advance_failed"
                    and event["workflow_state_after"] == "failed"
                    and "scheduler query unavailable" in event["reason"]
                    for event in events
                )
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

    def test_pipeline_terminal_notification_submits_once(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.notifications.records import read_notification_record
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        {
                            "notifications": {
                                "email": {
                                    "enabled": True,
                                    "recipients": [
                                        "you@example.com",
                                        "ops@example.com",
                                    ],
                                    "events": ["train_eval_pipeline_finished"],
                                    "when": "afterany",
                                }
                            }
                        }
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")

            advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            eval_submission = next(
                item
                for item in workflow_state["submissions"].values()
                if item["stage_name"] == "eval"
            )
            eval_root = Path(eval_submission["root"])
            eval_job_id = next(iter(eval_submission["groups"].values()))[
                "scheduler_job_id"
            ]
            self.assertEqual(execute_stage_task(eval_root, 1, 0), 0)
            client.set_array_task_state(eval_job_id, 0, "COMPLETED")

            result = advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["eval"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )
            advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["eval"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )

            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            control_submissions = json.loads(
                (pipeline_root / "control" / "control_submissions.json").read_text()
            )
            notify_submissions = [
                submission
                for submission in client.submissions
                if submission.path.name == "notify_train_eval_pipeline_finished.sbatch"
            ]
            self.assertEqual(len(notify_submissions), 2)
            self.assertEqual(notify_submissions[0].options.mail_user, "you@example.com")
            self.assertEqual(notify_submissions[1].options.mail_user, "ops@example.com")
            self.assertEqual(notify_submissions[0].options.mail_type, "END")
            record = read_notification_record(
                pipeline_root, "train_eval_pipeline_finished"
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
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.notifications.records import read_notification_record
        from tests.support.slurm import FakeSlurmClient

        class FailingNotificationSlurm(FakeSlurmClient):
            def submit(self, path, *, options=None):
                if path.name == "notify_train_eval_pipeline_finished.sbatch":
                    raise RuntimeError("mail scheduler unavailable")
                return super().submit(path, options=options)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        {
                            "notifications": {
                                "email": {
                                    "enabled": True,
                                    "recipients": ["you@example.com"],
                                    "events": ["train_eval_pipeline_finished"],
                                    "when": "afterany",
                                }
                            }
                        }
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            client = FailingNotificationSlurm()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")

            advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            eval_submission = next(
                item
                for item in workflow_state["submissions"].values()
                if item["stage_name"] == "eval"
            )
            eval_root = Path(eval_submission["root"])
            eval_job_id = next(iter(eval_submission["groups"].values()))[
                "scheduler_job_id"
            ]
            self.assertEqual(execute_stage_task(eval_root, 1, 0), 0)
            client.set_array_task_state(eval_job_id, 0, "COMPLETED")

            advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["eval"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            workflow_status = json.loads(
                (pipeline_root / "control" / "workflow_status.json").read_text()
            )
            notification_key = "terminal_notification:train_eval_pipeline_finished"
            self.assertEqual(workflow_state["terminal_aggregation"]["state"], "failed")
            self.assertEqual(
                workflow_status["control_jobs"][notification_key]["state"],
                "failed",
            )
            self.assertIn(
                "mail scheduler unavailable",
                workflow_status["control_jobs"][notification_key]["reason"],
            )

            recovery_client = FakeSlurmClient()
            advance_pipeline_once(pipeline_root, client=recovery_client)

            record = read_notification_record(
                pipeline_root, "train_eval_pipeline_finished"
            )
            assert record is not None
            self.assertEqual(record.state, "submitted")
            self.assertEqual(record.scheduler_job_ids, ("1001",))

    def test_terminal_notification_partial_submit_is_uncertain_and_not_retried(
        self,
    ) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

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
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=_with_current_python(
                        {
                            "notifications": {
                                "email": {
                                    "enabled": True,
                                    "recipients": [
                                        "you@example.com",
                                        "ops@example.com",
                                    ],
                                    "events": ["train_eval_pipeline_finished"],
                                    "when": "afterany",
                                }
                            }
                        }
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            client = PartiallyFailingNotificationSlurm()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0].job_id
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")

            advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["train"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            eval_submission = next(
                item
                for item in workflow_state["submissions"].values()
                if item["stage_name"] == "eval"
            )
            eval_root = Path(eval_submission["root"])
            eval_job_id = next(iter(eval_submission["groups"].values()))[
                "scheduler_job_id"
            ]
            self.assertEqual(execute_stage_task(eval_root, 1, 0), 0)
            client.set_array_task_state(eval_job_id, 0, "COMPLETED")

            advance_pipeline_once(
                pipeline_root,
                hint=_stage_finished(
                    plan.stage_batches["eval"].stage_instances[0].stage_instance_id
                ),
                client=client,
                missing_output_grace_seconds=0,
            )
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            control_submissions = json.loads(
                (pipeline_root / "control" / "control_submissions.json").read_text()
            )
            workflow_status = json.loads(
                (pipeline_root / "control" / "workflow_status.json").read_text()
            )
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
                workflow_status["control_jobs"][notification_key]["state"],
                "uncertain",
            )
            self.assertEqual(
                len(
                    workflow_status["control_jobs"][notification_key][
                        "scheduler_job_ids"
                    ]
                ),
                1,
            )
            submissions_before = len(client.submissions)
            advance_pipeline_once(pipeline_root, client=client)
            self.assertEqual(len(client.submissions), submissions_before)
