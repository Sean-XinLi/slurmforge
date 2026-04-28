from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from pathlib import Path


class RemovedEntrypointTests(StageBatchSystemTestCase):
    def test_removed_compatibility_entrypoints_stay_removed(self) -> None:
        import slurmforge.status as status
        import slurmforge.storage as storage

        self.assertFalse(hasattr(status, "batched_commits"))
        self.assertFalse(hasattr(storage, "load_stage_batch_plan"))
        self.assertFalse(hasattr(storage, "refresh_stage_batch_status"))
        self.assertFalse(Path("src/slurmforge/plans/loaders.py").exists())
        self.assertFalse(Path("src/slurmforge/storage/aggregate.py").exists())
        self.assertFalse(Path("src/slurmforge/storage/layout.py").exists())
        with self.assertRaises(ModuleNotFoundError):
            __import__("slurmforge.read_models")
        with self.assertRaises(ModuleNotFoundError):
            __import__(".".join(("slurmforge", "schema")))
        self.assertFalse(Path("src/slurmforge/spec/output_contract.py").exists())
        self.assertFalse(Path("src/slurmforge/schema").exists())

    def test_storage_loader_was_replaced_by_plan_reader(self) -> None:
        with self.assertRaises(ModuleNotFoundError):
            __import__(".".join(("slurmforge", "storage", "loader")))
        import slurmforge.storage.plan_reader as plan_reader

        for name in (
            "collect_stage_statuses",
            "is_stage_batch_root",
            "is_train_eval_pipeline_root",
            "iter_stage_run_dirs",
        ):
            self.assertFalse(hasattr(plan_reader, name), name)
