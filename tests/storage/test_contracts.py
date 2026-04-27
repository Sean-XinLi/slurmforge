from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    load_stage_batch_plan,
    write_demo_project,
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
from tests.support.std import Path, json, tempfile


class StorageContractTests(StageBatchSystemTestCase):
    def test_persisted_plan_and_output_contracts_require_schema_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

            batch_plan_path = Path(train_batch.submission_root) / "batch_plan.json"
            batch_payload = json.loads(batch_plan_path.read_text())
            removed_batch_schema = dict(batch_payload)
            del removed_batch_schema["schema_version"]
            batch_plan_path.write_text(json.dumps(removed_batch_schema), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "stage_batch_plan.schema_version is required"):
                load_stage_batch_plan(Path(train_batch.submission_root))

            batch_payload["schema_version"] = 1
            del batch_payload["stage_instances"][0]["output_contract"]["schema_version"]
            batch_plan_path.write_text(json.dumps(batch_payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "stage_output_contract.schema_version"):
                load_stage_batch_plan(Path(train_batch.submission_root))

    def test_stage_instance_plan_requires_current_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

            batch_plan_path = Path(train_batch.submission_root) / "batch_plan.json"
            batch_payload = json.loads(batch_plan_path.read_text())
            del batch_payload["stage_instances"][0]["resource_sizing"]
            batch_plan_path.write_text(json.dumps(batch_payload), encoding="utf-8")

            with self.assertRaisesRegex(KeyError, "resource_sizing"):
                load_stage_batch_plan(Path(train_batch.submission_root))

    def test_stage_batch_plan_requires_current_contract_fields(self) -> None:
        required_fields = ("group_plans", "budget_plan", "notification_plan")
        for field in required_fields:
            with self.subTest(field=field):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    spec = load_experiment_spec(write_demo_project(root))
                    train_batch = compile_stage_batch_for_kind(spec, kind="train")
                    write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

                    batch_plan_path = Path(train_batch.submission_root) / "batch_plan.json"
                    batch_payload = json.loads(batch_plan_path.read_text())
                    del batch_payload[field]
                    batch_plan_path.write_text(json.dumps(batch_payload), encoding="utf-8")

                    with self.assertRaisesRegex(KeyError, field):
                        load_stage_batch_plan(Path(train_batch.submission_root))

    def test_pipeline_plan_requires_current_controller_contract_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            pipeline = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(pipeline, spec_snapshot=spec.raw)

            pipeline_plan_path = Path(pipeline.root_dir) / "train_eval_pipeline_plan.json"
            payload = json.loads(pipeline_plan_path.read_text())
            del payload["controller_plan"]["runtime_plan"]
            pipeline_plan_path.write_text(json.dumps(payload), encoding="utf-8")

            from slurmforge.storage.loader import load_train_eval_pipeline_plan

            with self.assertRaisesRegex(KeyError, "runtime_plan"):
                load_train_eval_pipeline_plan(Path(pipeline.root_dir))

    def test_pipeline_plan_requires_train_eval_pipeline_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            pipeline = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(pipeline, spec_snapshot=spec.raw)

            pipeline_plan_path = Path(pipeline.root_dir) / "train_eval_pipeline_plan.json"
            payload = json.loads(pipeline_plan_path.read_text())
            del payload["pipeline_kind"]
            pipeline_plan_path.write_text(json.dumps(payload), encoding="utf-8")

            from slurmforge.storage.loader import load_train_eval_pipeline_plan

            with self.assertRaisesRegex(KeyError, "pipeline_kind"):
                load_train_eval_pipeline_plan(Path(pipeline.root_dir))
