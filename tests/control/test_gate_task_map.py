from __future__ import annotations

import tempfile
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_train_eval_pipeline_for_test
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    write_demo_project,
)


class GateTaskMapTests(StageBatchSystemTestCase):
    def test_gate_task_map_reader_is_strict(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.gate_task_map_contract import (
            gate_task_map_from_dict,
            stage_instance_id_for_task,
        )

        payload = {
            "schema_version": 1,
            "submission_id": "train_initial",
            "group_id": "group_001",
            "tasks": {"0": "run_1.train"},
        }

        record = gate_task_map_from_dict(payload)

        self.assertEqual(stage_instance_id_for_task(record, "0"), "run_1.train")
        with self.assertRaisesRegex(RecordContractError, "SLURM_ARRAY_TASK_ID"):
            stage_instance_id_for_task(record, "1")
        with self.assertRaisesRegex(RecordContractError, "submission_id"):
            gate_task_map_from_dict({**payload, "submission_id": ""})
        with self.assertRaisesRegex(RecordContractError, "tasks"):
            gate_task_map_from_dict({**payload, "tasks": {"0": ""}})

    def test_stage_instance_gate_array_writes_typed_task_map(self) -> None:
        from slurmforge.emit.pipeline_gate import (
            write_stage_instance_gate_array_submit_file,
        )
        from slurmforge.gate_task_map_contract import load_gate_task_map

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            instances = plan.stage_batches["train"].stage_instances

            write_stage_instance_gate_array_submit_file(
                plan,
                submission_id="train_initial",
                group_id="group_001",
                stage_instance_ids=tuple(
                    instance.stage_instance_id for instance in instances
                ),
            )

            record = load_gate_task_map(
                Path(plan.root_dir)
                / "control"
                / "gates"
                / "task_maps"
                / "train_initial_group_001.json"
            )
            self.assertEqual(record.submission_id, "train_initial")
            self.assertEqual(record.group_id, "group_001")
            self.assertEqual(record.tasks["0"], instances[0].stage_instance_id)
