from __future__ import annotations

import copy
import tempfile
from pathlib import Path

from slurmforge.storage.workflow_state_records import workflow_state_from_dict
from slurmforge.errors import RecordContractError
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
            }

            for name, invalid in cases.items():
                with self.subTest(name=name):
                    with self.assertRaises(RecordContractError):
                        workflow_state_from_dict(invalid)

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
