from __future__ import annotations

import json
import tempfile

from tests.control.pipeline_helpers import read_json
from tests.control.pipeline_overlays import grid_runs
from tests.control.pipeline_scenarios import advance_train_completion
from tests.control.pipeline_scenarios import build_train_eval_control_scenario
from tests.control.pipeline_scenarios import complete_train_task
from tests.control.pipeline_scenarios import submit_initial_and_complete_train_task
from tests.helpers.overlays import apply_overlay
from tests.support.case import StageBatchSystemTestCase


class PipelineReleasePolicyTests(StageBatchSystemTestCase):
    def test_per_run_release_dispatches_one_eval_while_other_train_tasks_run(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=apply_overlay(
                    grid_runs(4),
                    {"dispatch": {"max_available_gpus": 4}},
                ),
            )
            train_job_id = submit_initial_and_complete_train_task(scenario)
            for task_index in (1, 2, 3):
                scenario.client.set_array_task_state(
                    train_job_id,
                    task_index,
                    "RUNNING",
                )

            plan = scenario.plan
            first_train = plan.stage_batches["train"].stage_instances[0]
            first_eval = plan.stage_batches["eval"].stage_instances[0]
            result = advance_train_completion(scenario)
            advance_train_completion(scenario)

            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            events = [
                json.loads(line)
                for line in (scenario.pipeline_root / "control" / "events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(result.state, "streaming")
            self.assertEqual(len(scenario.client.submissions), 6)
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
        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=apply_overlay(
                    grid_runs(2),
                    {"dispatch": {"max_available_gpus": 2}},
                ),
            )
            train_job_id = submit_initial_and_complete_train_task(scenario)
            scenario.client.set_array_task_state(train_job_id, 1, "FAILED")

            advance_train_completion(scenario)

            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            plan = scenario.plan
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
        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=apply_overlay(
                    grid_runs(2),
                    {
                        "dispatch": {
                            "max_available_gpus": 2,
                            "release_policy": "per_group",
                        }
                    },
                ),
            )
            train_job_id = submit_initial_and_complete_train_task(scenario)
            scenario.client.set_array_task_state(train_job_id, 1, "RUNNING")

            advance_train_completion(scenario)
            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            plan = scenario.plan
            for eval_instance in plan.stage_batches["eval"].stage_instances:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "planned",
                )

            complete_train_task(scenario, train_job_id, task_index=1)
            advance_train_completion(scenario, train_index=1)
            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            for eval_instance in plan.stage_batches["eval"].stage_instances:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "submitted",
                )

    def test_windowed_release_batches_ready_eval_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scenario = build_train_eval_control_scenario(
                tmp,
                extra=apply_overlay(
                    grid_runs(2),
                    {
                        "dispatch": {
                            "max_available_gpus": 2,
                            "release_policy": "windowed",
                            "window_size": 2,
                            "window_seconds": 999,
                        }
                    },
                ),
            )
            train_job_id = submit_initial_and_complete_train_task(scenario)
            scenario.client.set_array_task_state(train_job_id, 1, "RUNNING")

            advance_train_completion(scenario)
            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            plan = scenario.plan
            self.assertEqual(
                workflow_state["instances"][
                    plan.stage_batches["eval"].stage_instances[0].stage_instance_id
                ]["state"],
                "ready",
            )

            complete_train_task(scenario, train_job_id, task_index=1)
            advance_train_completion(scenario, train_index=1)
            workflow_state = read_json(
                scenario.pipeline_root / "control" / "workflow_state.json"
            )
            for eval_instance in plan.stage_batches["eval"].stage_instances:
                self.assertEqual(
                    workflow_state["instances"][eval_instance.stage_instance_id][
                        "state"
                    ],
                    "submitted",
                )
