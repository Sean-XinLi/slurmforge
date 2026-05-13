from __future__ import annotations

import ast
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class StorageShapeTests(StageBatchSystemTestCase):
    def test_root_path_inference_has_single_source(self) -> None:
        self.assertTrue(Path("src/slurmforge/root_paths.py").exists())
        self.assertFalse(Path("src/slurmforge/root_model/paths.py").exists())
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if (
                "_pipeline_root_for_batch_root" in text
                or "_parent_pipeline_root" in text
            ):
                violations.append(str(path))
            if "from .paths import parent_pipeline_root_for_stage_batch" in text:
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_storage_layout_is_split_by_root_type(self) -> None:
        self.assertFalse(Path("src/slurmforge/storage/layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/batch_layout.py").exists())
        self.assertTrue(
            Path("src/slurmforge/storage/train_eval_pipeline_layout.py").exists()
        )
        self.assertFalse(
            Path("src/slurmforge/storage", "status_" + "seed.py").exists()
        )
        self.assertFalse(Path("src/slurmforge/storage/loader.py").exists())
        self.assertFalse(Path("src/slurmforge/storage/execution_index.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/batch_registry.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/execution_catalog.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/runtime_batches.py").exists())
        registry_text = Path("src/slurmforge/storage/batch_registry.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("class BatchRegistryRecord", registry_text)
        self.assertIn("class BatchRegistry", registry_text)
        self.assertTrue(Path("src/slurmforge/storage/plan_reader.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/workflow.py").exists())
        self.assertTrue(Path("src/slurmforge/root_model/seed.py").exists())

    def test_stage_run_path_conventions_have_single_owner(self) -> None:
        owner = Path("src/slurmforge/storage/paths.py")
        self.assertTrue(owner.exists())
        owner_text = owner.read_text(encoding="utf-8")
        for name in (
            "status_path",
            "stage_plan_path",
            "input_bindings_path",
            "input_verification_path",
            "stage_outputs_path",
            "status_events_path",
            "attempts_dir",
            "attempt_path",
            "root_ref_path",
            "next_attempt_id",
        ):
            self.assertIn(f"def {name}(", owner_text)

        forbidden_defs = (
            "def status_path(",
            "def input_verification_path(",
            "def stage_outputs_path(",
            "def root_ref_path(",
            "def attempts_dir(",
            "def attempt_path(",
            "def _status_events_path(",
            "def _stage_outputs_path(",
            "def _root_ref_path(",
            "def _attempts_dir(",
            "def _next_attempt_id(",
            "def _stage_plan_path(",
            "def _input_bindings_path(",
            "def _manifest_path(",
        )
        violations = [
            f"{path}: {pattern}"
            for path in sorted(Path("src/slurmforge").rglob("*.py"))
            if path != owner
            for pattern in forbidden_defs
            if pattern in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_json_record_writers_use_object_boundary(self) -> None:
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text, filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if alias.name == "write_json":
                            violations.append(f"{path}:{node.lineno}: import write_json")
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    if node.func.id == "write_json":
                        violations.append(f"{path}:{node.lineno}: write_json()")
                elif isinstance(node, ast.Constant) and node.value == "write_json":
                    violations.append(f"{path}:{node.lineno}: write_json export")
        self.assertEqual(violations, [])

    def test_materialization_workflow_is_not_owned_by_storage(self) -> None:
        storage_root = Path("src/slurmforge/storage")
        self.assertFalse((storage_root / "materialization.py").exists())
        self.assertTrue(
            (storage_root / "batch_materialization_records.py").exists()
        )
        self.assertTrue((storage_root / "derived_roots.py").exists())
        self.assertTrue(Path("src/slurmforge/materialization/stage_batch.py").exists())
        self.assertTrue(Path("src/slurmforge/materialization/train_eval.py").exists())
        self.assertTrue(Path("src/slurmforge/materialization/sourced.py").exists())

    def test_strict_record_readers_do_not_default_missing_fields(self) -> None:
        strict_readers = (
            "src/slurmforge/control/control_submission_ledger.py",
            "src/slurmforge/control/control_submission_records.py",
            "src/slurmforge/lineage/records.py",
            "src/slurmforge/notifications/records.py",
            "src/slurmforge/outputs/models.py",
            "src/slurmforge/plans/serde/outputs.py",
            "src/slurmforge/plans/serde/resources.py",
            "src/slurmforge/plans/serde/runtime.py",
            "src/slurmforge/plans/serde/train_eval.py",
            "src/slurmforge/root_model/manifest.py",
            "src/slurmforge/root_model/root_ref.py",
            "src/slurmforge/status/reconcile_observations.py",
            "src/slurmforge/status/serde.py",
            "src/slurmforge/storage/batch_materialization_records.py",
            "src/slurmforge/storage/batch_registry.py",
            "src/slurmforge/storage/workflow_state_serde.py",
            "src/slurmforge/storage/workflow_status_records.py",
            "src/slurmforge/submission/ledger_records.py",
            "src/slurmforge/submission/submit_manifest.py",
        )
        violations = [
            path
            for path in strict_readers
            if "payload.get(" in Path(path).read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_remaining_storage_boundary_readers_are_strict(self) -> None:
        files = (
            "src/slurmforge/lineage/query.py",
            "src/slurmforge/root_model/root_ref.py",
            "src/slurmforge/submission/submit_manifest.py",
        )
        forbidden = (
            "payload.get(",
            "index.get(",
            "item.get(",
            "manifest.get(",
            " in (None, \"\")",
            "str(payload[",
            "str(manifest[",
        )
        violations = [
            f"{path}: {pattern}"
            for path in files
            for pattern in forbidden
            if pattern in Path(path).read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_submit_manifest_reader_is_not_owned_by_emit(self) -> None:
        emit_stage = Path("src/slurmforge/emit/stage.py").read_text(encoding="utf-8")
        self.assertNotIn("def load_stage_submit_manifest", emit_stage)
        self.assertNotIn("def _manifest_path", emit_stage)
        self.assertNotIn("read_json", emit_stage)
        self.assertTrue(Path("src/slurmforge/submission/submit_manifest.py").exists())

    def test_root_ref_reader_does_not_infer_missing_records(self) -> None:
        root_ref = Path("src/slurmforge/root_model/root_ref.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("infer_stage_root_ref", root_ref)
        self.assertNotIn("parent_pipeline_root_for_stage_batch", root_ref)

    def test_plan_serde_readers_do_not_coerce_payload_scalars(self) -> None:
        reader_files = (
            "src/slurmforge/plans/serde/budget.py",
            "src/slurmforge/plans/serde/launcher.py",
            "src/slurmforge/plans/serde/notifications.py",
            "src/slurmforge/plans/serde/stage.py",
        )
        forbidden = (
            "str(payload[",
            "int(payload[",
            "bool(payload[",
            "str(item) for item in",
            "int(item) for item in",
            "bool(item) for item in",
            " in (None, \"\")",
        )
        violations = [
            f"{path}: {pattern}"
            for path in reader_files
            for pattern in forbidden
            if pattern in Path(path).read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_contract_from_dict_readers_use_record_helpers(self) -> None:
        readers = {
            "src/slurmforge/contracts/inputs.py": (
                "input_source_from_dict",
                "resolved_input_from_dict",
                "input_binding_from_dict",
            ),
            "src/slurmforge/contracts/outputs.py": (
                "output_discovery_rule_from_dict",
                "stage_output_spec_from_dict",
                "stage_output_contract_from_dict",
            ),
        }
        forbidden_getters = ("payload.get(", "values.get(")
        violations: list[str] = []
        for path, function_names in readers.items():
            text = Path(path).read_text(encoding="utf-8")
            lines = text.splitlines()
            tree = ast.parse(text)
            functions = {
                node.name: "\n".join(lines[node.lineno - 1 : node.end_lineno])
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef)
            }
            for function_name in function_names:
                source = functions[function_name]
                for pattern in forbidden_getters:
                    if pattern in source:
                        violations.append(f"{path}:{function_name}: {pattern}")
                function_tree = ast.parse(source)
                for node in ast.walk(function_tree):
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                        if node.func.id in {"str", "int", "bool"}:
                            violations.append(
                                f"{path}:{function_name}: {node.func.id}()"
                            )
        self.assertEqual(violations, [])

    def test_stage_outputs_boundary_uses_typed_records(self) -> None:
        records_text = Path("src/slurmforge/outputs/records.py").read_text(
            encoding="utf-8"
        )
        resolver_text = Path("src/slurmforge/resolver/output_refs.py").read_text(
            encoding="utf-8"
        )
        inputs_text = Path("src/slurmforge/contracts/inputs.py").read_text(
            encoding="utf-8"
        )
        contracts_init = Path("src/slurmforge/contracts/__init__.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("StageOutputsRecord | None", records_text)
        self.assertNotIn("return payload", records_text)
        self.assertNotIn("payload.get(", resolver_text)
        self.assertNotIn("output.get(", resolver_text)
        self.assertNotIn("resolved_input_from_output_ref", inputs_text)
        self.assertNotIn("resolved_input_from_output_ref", contracts_init)

    def test_input_verification_boundary_uses_typed_records(self) -> None:
        models_text = Path("src/slurmforge/inputs/models.py").read_text(
            encoding="utf-8"
        )
        serde_text = Path("src/slurmforge/inputs/serde.py").read_text(
            encoding="utf-8"
        )
        ready_text = Path("src/slurmforge/submission/ready.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("source: InputSource", models_text)
        self.assertNotIn("source: JsonObject", models_text)
        self.assertIn("stage_input_verification_report_from_dict", serde_text)
        self.assertNotIn("read_json_object", ready_text)
        self.assertNotIn("verification.get(", ready_text)

    def test_lineage_boundary_uses_typed_records(self) -> None:
        records_text = Path("src/slurmforge/lineage/records.py").read_text(
            encoding="utf-8"
        )
        paths_text = Path("src/slurmforge/lineage/paths.py").read_text(
            encoding="utf-8"
        )
        query_text = Path("src/slurmforge/lineage/query.py").read_text(
            encoding="utf-8"
        )
        resolver_text = Path("src/slurmforge/resolver/upstream.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("LineageIndexRecord", records_text)
        self.assertIn("def lineage_index_from_dict", records_text)
        self.assertNotIn(
            "def lineage_index_from_dict(payload: dict[str, Any]) -> dict",
            records_text,
        )
        self.assertIn("LineageIndexRecord | None", paths_text)
        self.assertNotIn('index["', query_text)
        self.assertNotIn('item["', query_text)
        self.assertNotIn("return dict(item)", query_text)
        self.assertIn("LineageInputSourceRecord", resolver_text)
        self.assertNotIn("required_object(record", resolver_text)

    def test_root_manifest_detection_uses_strict_reader(self) -> None:
        manifest_text = Path("src/slurmforge/root_model/manifest.py").read_text(
            encoding="utf-8"
        )
        detection_text = Path("src/slurmforge/root_model/detection.py").read_text(
            encoding="utf-8"
        )
        models_text = Path("src/slurmforge/root_model/models.py").read_text(
            encoding="utf-8"
        )
        manifest_model_text = manifest_text.split("def root_manifest_path", 1)[0]
        descriptor_model_text = models_text.split("class RootStatusSnapshot", 1)[0]
        root_paths_text = Path("src/slurmforge/root_paths.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("class RootManifestRecord", manifest_text)
        self.assertIn("schema_version: int", manifest_text)
        self.assertIn("schema_version: int", models_text)
        self.assertIn("read_root_manifest", detection_text)
        self.assertIn("read_root_manifest", root_paths_text)
        self.assertNotIn("    payload:", manifest_model_text)
        self.assertNotIn("    manifest:", descriptor_model_text)
        self.assertNotIn("manifest.payload", detection_text)
        self.assertNotIn('manifest.get("kind")', detection_text)
        self.assertNotIn('manifest.get("kind")', root_paths_text)
        self.assertNotIn('payload.get("kind")', detection_text)
        self.assertNotIn('payload.get("kind")', root_paths_text)
