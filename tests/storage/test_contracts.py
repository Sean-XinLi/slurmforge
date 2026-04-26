from __future__ import annotations

from tests.support import *  # noqa: F401,F403


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

