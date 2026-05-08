from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import materialize_stage_batch_for_test
import json
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch


class OutputTests(StageBatchSystemTestCase):
    def test_invalid_artifact_strategy_is_config_contract_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.outputs.artifact_store import manage_file
        from slurmforge.plans.outputs import ArtifactStorePlan

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "checkpoint.pt"
            source.write_text("checkpoint\n", encoding="utf-8")

            with self.assertRaisesRegex(
                ConfigContractError, "Unsupported artifact store strategy"
            ):
                manage_file(
                    str(source),
                    attempt_dir=root / "attempt",
                    kind="file",
                    store_plan=ArtifactStorePlan(
                        strategy="bad-strategy", verify_digest=False
                    ),
                )

    def test_invalid_metric_json_path_is_config_contract_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.outputs.selection import json_path_value

        with self.assertRaisesRegex(
            ConfigContractError, "unsupported metric json_path"
        ):
            json_path_value({"accuracy": 0.9}, "accuracy")

    def test_artifact_store_hardlink_strategy_is_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "artifact_store": {
                        "strategy": "hardlink",
                        "fallback_strategy": "copy",
                        "verify_digest": True,
                    }
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(batch.submission_root), 1, 0), 0)
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            manifest = json.loads(
                (
                    run_dir
                    / "attempts"
                    / "0001"
                    / "artifacts"
                    / "artifact_manifest.json"
                ).read_text()
            )
            self.assertIn(manifest["artifacts"][0]["strategy"], {"hardlink", "copy"})
            self.assertTrue(manifest["artifacts"][0]["verified"])

    def test_artifact_digest_mismatch_fails_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            with patch(
                "slurmforge.outputs.artifact_store.file_digest",
                side_effect=["source", "managed"],
            ):
                self.assertNotEqual(
                    execute_stage_task(Path(batch.submission_root), 1, 0), 0
                )
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            status = json.loads((run_dir / "status.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "artifact_integrity_error")

    def test_required_artifact_contract_failure_blocks_stage_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            cfg = yaml.safe_load(cfg_path.read_text())
            cfg["stages"]["train"]["outputs"]["required_log"] = {
                "kind": "files",
                "discover": {"globs": ["required/**/*.log"]},
                "required": True,
            }
            cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            self.assertNotEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            status = json.loads((run_dir / "status.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "missing_output")
            self.assertIn(
                "required output `required_log` was not produced", status["reason"]
            )
            self.assertFalse((run_dir / "stage_outputs.json").exists())
            self.assertTrue(
                (
                    run_dir
                    / "attempts"
                    / "0001"
                    / "artifacts"
                    / "artifact_manifest.json"
                ).exists()
            )

    def test_artifact_records_reject_missing_or_wrong_typed_contract_fields(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.io import SchemaVersion
        from slurmforge.outputs.models import (
            artifact_manifest_record_from_dict,
            artifact_ref_from_dict,
        )

        artifact_payload = {
            "schema_version": SchemaVersion.OUTPUT_RECORD,
            "name": "checkpoint",
            "kind": "file",
            "source_path": "/tmp/checkpoint.pt",
            "managed_path": "/tmp/artifacts/checkpoint.pt",
            "strategy": "copy",
            "managed": True,
            "digest": "abc",
            "source_digest": "abc",
            "managed_digest": "abc",
            "verified": None,
            "size_bytes": 12,
            "optional": False,
        }
        artifact_cases = {
            "missing_source_digest": {
                key: value
                for key, value in artifact_payload.items()
                if key != "source_digest"
            },
            "string_verified": {**artifact_payload, "verified": "true"},
            "integer_optional": {**artifact_payload, "optional": 0},
        }
        for name, payload in artifact_cases.items():
            with self.subTest(name=name):
                with self.assertRaises(RecordContractError):
                    artifact_ref_from_dict(payload)

        manifest_payload = {
            "schema_version": SchemaVersion.OUTPUT_RECORD,
            "stage_instance_id": "run_1.train",
            "attempt_id": "0001",
            "artifacts": [artifact_payload],
        }
        manifest_cases = {
            "missing_artifacts": {
                key: value
                for key, value in manifest_payload.items()
                if key != "artifacts"
            },
            "artifact_not_object": {**manifest_payload, "artifacts": ["bad"]},
        }
        for name, payload in manifest_cases.items():
            with self.subTest(name=name):
                with self.assertRaises(RecordContractError):
                    artifact_manifest_record_from_dict(payload)

    def test_stage_outputs_reader_returns_typed_record(self) -> None:
        from slurmforge.outputs.records import load_stage_outputs
        from slurmforge.plans.outputs import OutputRef, StageOutputsRecord

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(batch.submission_root), 1, 0), 0)
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel

            outputs = load_stage_outputs(run_dir)

            self.assertIsInstance(outputs, StageOutputsRecord)
            assert outputs is not None
            self.assertIsInstance(outputs.outputs["checkpoint"], OutputRef)
            self.assertEqual(outputs.outputs["checkpoint"].output_name, "checkpoint")

    def test_stage_outputs_record_rejects_invalid_outputs_map(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.io import SchemaVersion
        from slurmforge.plans.serde import stage_outputs_record_from_dict
        from slurmforge.resolver.output_refs import output_ref

        output_payload = {
            "schema_version": SchemaVersion.OUTPUT_RECORD,
            "output_name": "checkpoint",
            "kind": "file",
            "path": "/tmp/checkpoint.pt",
            "producer_stage_instance_id": "run_1.train",
            "cardinality": "one",
            "producer_attempt_id": "0001",
            "digest": "digest",
            "source_path": "/tmp/source.pt",
            "managed": True,
            "strategy": "copy",
            "source_digest": "source",
            "managed_digest": "managed",
            "verified": True,
            "size_bytes": 4,
            "selection_reason": "latest_step",
            "value": None,
        }
        payload = {
            "schema_version": SchemaVersion.OUTPUT_RECORD,
            "stage_instance_id": "run_1.train",
            "producer_attempt_id": "0001",
            "outputs": {"checkpoint": output_payload},
            "artifacts": [],
            "artifact_manifest": "/tmp/artifact_manifest.json",
        }

        record = stage_outputs_record_from_dict(payload)
        self.assertIsNotNone(output_ref(record, "checkpoint"))
        empty_path = stage_outputs_record_from_dict(
            {
                **payload,
                "outputs": {
                    "checkpoint": {
                        **output_payload,
                        "path": "",
                    }
                },
            }
        )
        self.assertIsNone(output_ref(empty_path, "checkpoint"))

        with self.assertRaisesRegex(RecordContractError, "does not match"):
            stage_outputs_record_from_dict(
                {
                    **payload,
                    "outputs": {"renamed": output_payload},
                }
            )
        with self.assertRaisesRegex(RecordContractError, "must be an object"):
            stage_outputs_record_from_dict(
                {
                    **payload,
                    "outputs": {"checkpoint": "bad"},
                }
            )
        with self.assertRaisesRegex(RecordContractError, "non-empty strings"):
            stage_outputs_record_from_dict(
                {
                    **payload,
                    "outputs": {"": output_payload},
                }
            )
