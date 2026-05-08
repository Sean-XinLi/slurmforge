from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import (
    load_stage_batch_plan,
    materialize_train_eval_pipeline_for_test,
    materialize_stage_batch_for_test,
)
from slurmforge.errors import RecordContractError
import copy
import json
import tempfile
from pathlib import Path


class StorageContractTests(StageBatchSystemTestCase):
    def test_persisted_plan_and_output_contracts_require_schema_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            batch_plan_path = Path(train_batch.submission_root) / "batch_plan.json"
            batch_payload = json.loads(batch_plan_path.read_text())
            invalid_batch_schema = dict(batch_payload)
            del invalid_batch_schema["schema_version"]
            batch_plan_path.write_text(
                json.dumps(invalid_batch_schema), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                RecordContractError, "stage_batch_plan.schema_version is required"
            ):
                load_stage_batch_plan(Path(train_batch.submission_root))

            batch_payload["schema_version"] = 1
            del batch_payload["stage_instances"][0]["output_contract"]["schema_version"]
            batch_plan_path.write_text(json.dumps(batch_payload), encoding="utf-8")
            with self.assertRaisesRegex(
                RecordContractError, "stage_output_contract.schema_version"
            ):
                load_stage_batch_plan(Path(train_batch.submission_root))

    def test_stage_instance_plan_requires_current_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            batch_plan_path = Path(train_batch.submission_root) / "batch_plan.json"
            batch_payload = json.loads(batch_plan_path.read_text())
            del batch_payload["stage_instances"][0]["resource_sizing"]
            batch_plan_path.write_text(json.dumps(batch_payload), encoding="utf-8")

            with self.assertRaisesRegex(RecordContractError, "resource_sizing"):
                load_stage_batch_plan(Path(train_batch.submission_root))

    def test_stage_batch_plan_requires_current_contract_fields(self) -> None:
        required_fields = ("group_plans", "budget_plan", "notification_plan")
        for field in required_fields:
            with self.subTest(field=field):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    spec = load_experiment_spec(write_demo_project(root))
                    train_batch = compile_stage_batch_for_kind(spec, kind="train")
                    materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

                    batch_plan_path = (
                        Path(train_batch.submission_root) / "batch_plan.json"
                    )
                    batch_payload = json.loads(batch_plan_path.read_text())
                    del batch_payload[field]
                    batch_plan_path.write_text(
                        json.dumps(batch_payload), encoding="utf-8"
                    )

                    with self.assertRaisesRegex(RecordContractError, field):
                        load_stage_batch_plan(Path(train_batch.submission_root))

    def test_stage_batch_plan_rejects_coerced_scalar_records(self) -> None:
        def set_stage_instance_run_index(payload: dict) -> None:
            payload["stage_instances"][0]["run_index"] = "0"

        def set_launcher_args(payload: dict) -> None:
            payload["stage_instances"][0]["launcher_plan"]["args"] = [1]

        def set_budget_max_available_gpus(payload: dict) -> None:
            payload["budget_plan"]["max_available_gpus"] = "8"

        def set_group_array_throttle(payload: dict) -> None:
            payload["group_plans"][0]["array_throttle"] = ""

        def set_notification_enabled(payload: dict) -> None:
            payload["notification_plan"]["email"]["enabled"] = "false"

        cases = (
            ("run_index", set_stage_instance_run_index),
            ("args", set_launcher_args),
            ("max_available_gpus", set_budget_max_available_gpus),
            ("array_throttle", set_group_array_throttle),
            ("enabled", set_notification_enabled),
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            batch_plan_path = Path(train_batch.submission_root) / "batch_plan.json"
            original_payload = json.loads(batch_plan_path.read_text())

            for expected_field, mutate in cases:
                with self.subTest(field=expected_field):
                    batch_payload = copy.deepcopy(original_payload)
                    mutate(batch_payload)
                    batch_plan_path.write_text(
                        json.dumps(batch_payload), encoding="utf-8"
                    )

                    with self.assertRaisesRegex(RecordContractError, expected_field):
                        load_stage_batch_plan(Path(train_batch.submission_root))

    def test_pipeline_plan_requires_current_control_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            pipeline = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(pipeline, spec_snapshot=spec.raw)

            pipeline_plan_path = (
                Path(pipeline.root_dir) / "train_eval_pipeline_plan.json"
            )
            payload = json.loads(pipeline_plan_path.read_text())
            del payload["control_plan"]["runtime_plan"]
            pipeline_plan_path.write_text(json.dumps(payload), encoding="utf-8")

            from slurmforge.storage.plan_reader import load_train_eval_pipeline_plan
            with self.assertRaisesRegex(RecordContractError, "runtime_plan"):
                load_train_eval_pipeline_plan(Path(pipeline.root_dir))

    def test_pipeline_plan_requires_train_eval_pipeline_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            pipeline = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(pipeline, spec_snapshot=spec.raw)

            pipeline_plan_path = (
                Path(pipeline.root_dir) / "train_eval_pipeline_plan.json"
            )
            payload = json.loads(pipeline_plan_path.read_text())
            del payload["pipeline_kind"]
            pipeline_plan_path.write_text(json.dumps(payload), encoding="utf-8")

            from slurmforge.storage.plan_reader import load_train_eval_pipeline_plan
            with self.assertRaisesRegex(RecordContractError, "pipeline_kind"):
                load_train_eval_pipeline_plan(Path(pipeline.root_dir))

    def test_write_batch_registry_does_not_mutate_input_record(self) -> None:
        from slurmforge.storage.batch_registry import (
            BatchRegistry,
            BatchRegistryRecord,
            write_batch_registry,
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "batch_registry.json"
            registry = BatchRegistry(
                schema_version=0,
                pipeline_id="pipe",
                updated_at="old",
                batches=[
                    BatchRegistryRecord(
                        stage_name="train",
                        role="pipeline_stage",
                        dispatch_id="",
                        stage_batch_root="/tmp/train",
                        batch_id="batch",
                        source_ref="source",
                        source_dispatch_id="",
                        run_ids=("run_1",),
                        stage_instance_ids=("run_1.train",),
                        group_ids=("g0",),
                        updated_at="record-old",
                    )
                ],
            )

            write_batch_registry(path, registry, schema_version=7)

            self.assertEqual(registry.schema_version, 0)
            self.assertEqual(registry.updated_at, "old")
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], 7)
            self.assertEqual(payload["pipeline_id"], "pipe")
            self.assertNotEqual(payload["updated_at"], "old")

    def test_batch_registry_rejects_non_json_array_and_non_string_items(self) -> None:
        from slurmforge.storage.batch_registry import (
            batch_registry_record_from_dict,
            read_batch_registry,
        )

        record_payload = {
            "stage_name": "train",
            "role": "pipeline_stage",
            "dispatch_id": "",
            "stage_batch_root": "/tmp/train",
            "batch_id": "batch",
            "source_ref": "source",
            "source_dispatch_id": "",
            "run_ids": ("run_1",),
            "stage_instance_ids": ["run_1.train"],
            "group_ids": ["g0"],
            "updated_at": "2026-01-01T00:00:00Z",
        }
        with self.assertRaises(RecordContractError):
            batch_registry_record_from_dict(record_payload)

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "batch_registry.json"
            payload = {
                "schema_version": 7,
                "pipeline_id": "pipe",
                "updated_at": "2026-01-01T00:00:00Z",
                "batches": [
                    {
                        **record_payload,
                        "run_ids": [1],
                    }
                ],
            }
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaises(RecordContractError):
                read_batch_registry(path, schema_version=7)
