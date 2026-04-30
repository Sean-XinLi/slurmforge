from __future__ import annotations

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
