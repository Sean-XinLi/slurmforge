from __future__ import annotations

import copy
import tempfile
from pathlib import Path

from slurmforge.errors import RecordContractError
from slurmforge.storage.workflow_state_serde import (
    workflow_state_from_dict,
    workflow_state_to_dict,
)
from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_train_eval_pipeline_for_test
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.slurm import FakeSlurmClient
from tests.support.workflow_records import read_workflow_state_payload


class WorkflowStateContractTests(StageBatchSystemTestCase):
    def test_malformed_initial_workflow_state_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload = self._initial_payload(Path(tmp))
            instance_id = next(iter(payload["instances"]))
            dependency_id = next(iter(payload["dependencies"]))

            cases = {
                "missing_instances": _without(payload, "instances"),
                "invalid_release_policy": {
                    **payload,
                    "release_policy": "soon",
                },
                "instance_key_mismatch": _with_nested(
                    payload,
                    ("instances", instance_id, "stage_instance_id"),
                    "wrong",
                ),
                "dependency_unknown_downstream": _with_nested(
                    payload,
                    ("dependencies", dependency_id, "downstream_instance_id"),
                    "eval/missing",
                ),
                "terminal_state_with_nonterminal_instances": {
                    **payload,
                    "state": "success",
                },
                "dispatch_queue_not_json_array": {
                    **payload,
                    "dispatch_queue": (),
                },
                "dispatch_queue_non_string_item": {
                    **payload,
                    "dispatch_queue": [1],
                },
            }

            for name, invalid in cases.items():
                with self.subTest(name=name):
                    with self.assertRaises(RecordContractError):
                        workflow_state_from_dict(invalid)

    def test_malformed_submitted_workflow_state_is_rejected(self) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            submit_initial_pipeline(plan, client=FakeSlurmClient())
            payload = read_workflow_state_payload(Path(plan.root_dir))
            submission_id = next(iter(payload["submissions"]))
            group_id = next(iter(payload["submissions"][submission_id]["groups"]))
            instance_id = next(
                instance_id
                for instance_id, instance in payload["instances"].items()
                if instance["state"] == "submitted"
            )

            cases = {
                "submission_missing_display_key": _without_nested(
                    payload,
                    ("submissions", submission_id, "display_key"),
                ),
                "submitted_instance_without_scheduler_job_id": _with_nested(
                    payload,
                    ("instances", instance_id, "scheduler_job_id"),
                    "",
                ),
                "submission_instance_id_non_string": _with_nested(
                    payload,
                    ("submissions", submission_id, "instance_ids"),
                    [1],
                ),
                "group_stage_instance_id_non_string": _with_nested(
                    payload,
                    (
                        "submissions",
                        submission_id,
                        "groups",
                        group_id,
                        "stage_instance_ids",
                    ),
                    [1],
                ),
                "group_task_id_key_non_string": _with_nested(
                    payload,
                    (
                        "submissions",
                        submission_id,
                        "groups",
                        group_id,
                        "task_ids_by_instance",
                    ),
                    {1: "0"},
                ),
                "group_task_id_value_non_string": _with_nested(
                    payload,
                    (
                        "submissions",
                        submission_id,
                        "groups",
                        group_id,
                        "task_ids_by_instance",
                    ),
                    {instance_id: 0},
                ),
            }

            for name, invalid in cases.items():
                with self.subTest(name=name):
                    with self.assertRaises(RecordContractError):
                        workflow_state_from_dict(invalid)

    def test_workflow_state_to_dict_rejects_malformed_in_memory_records(self) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            submit_initial_pipeline(plan, client=FakeSlurmClient())
            payload = read_workflow_state_payload(Path(plan.root_dir))
            base_state = workflow_state_from_dict(payload)
            instance_id = next(iter(base_state.instances))

            def list_dispatch_queue(state) -> None:
                state.dispatch_queue = [instance_id]

            def list_submission_instance_ids(state) -> None:
                submission = next(iter(state.submissions.values()))
                submission.instance_ids = [instance_id]

            def list_group_stage_instance_ids(state) -> None:
                group = _first_group(state)
                group.stage_instance_ids = [instance_id]

            def non_string_task_key(state) -> None:
                group = _first_group(state)
                group.task_ids_by_instance = {1: "0"}

            def bool_array_size(state) -> None:
                group = _first_group(state)
                group.array_size = True

            def non_bool_output_ready(state) -> None:
                instance = state.instances[instance_id]
                instance.output_ready = 1

            cases = {
                "list_dispatch_queue": list_dispatch_queue,
                "list_submission_instance_ids": list_submission_instance_ids,
                "list_group_stage_instance_ids": list_group_stage_instance_ids,
                "non_string_task_key": non_string_task_key,
                "bool_array_size": bool_array_size,
                "non_bool_output_ready": non_bool_output_ready,
            }

            for name, mutate in cases.items():
                with self.subTest(name=name):
                    state = copy.deepcopy(base_state)
                    mutate(state)
                    with self.assertRaises(RecordContractError):
                        workflow_state_to_dict(state)

    def _initial_payload(self, root: Path) -> dict:
        spec = load_experiment_spec(write_demo_project(root))
        plan = compile_train_eval_pipeline_plan(spec)
        materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
        return read_workflow_state_payload(Path(plan.root_dir))


def _without(payload: dict, key: str) -> dict:
    result = copy.deepcopy(payload)
    del result[key]
    return result


def _without_nested(payload: dict, path: tuple[str, ...]) -> dict:
    result = copy.deepcopy(payload)
    target = result
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    return result


def _with_nested(payload: dict, path: tuple[str, ...], value) -> dict:
    result = copy.deepcopy(payload)
    target = result
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return result


def _first_group(state):
    submission = next(iter(state.submissions.values()))
    return next(iter(submission.groups.values()))
