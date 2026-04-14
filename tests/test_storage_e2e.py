"""End-to-end tests for the storage layer across engine modes.

Test matrix:
  1. engine=none  (filesystem only)
  2. engine=sqlite + planning_recovery=true  (DB + recovery files)
  3. engine=sqlite + planning_recovery=false (pure DB, no recovery files)

Each flow: generate → load_plan → status (list_batch_run_views) → replay dry-path
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from slurmforge.errors import ConfigContractError
from slurmforge.pipeline.config.codecs import normalize_storage_config
from slurmforge.pipeline.materialization import materialize_batch
from slurmforge.pipeline.planning import BatchIdentity, PlannedBatch, PlannedRun
from slurmforge.storage import (
    create_planning_store,
    create_planning_store_for_read,
    open_batch_storage,
    storage_config_for_batch,
)
from slurmforge.storage.backends.sqlite.connection import db_path_for_batch
from slurmforge.storage.descriptor import read_storage_descriptor
from tests._support import make_template_env, sample_run_plan, sample_run_snapshot, sample_stage_plan


def _build_planned_batch(tmp_path: Path, storage_cfg_dict: dict | None = None) -> PlannedBatch:
    storage_config = normalize_storage_config(storage_cfg_dict)
    identity = BatchIdentity(
        project_root=tmp_path,
        base_output_dir=tmp_path / "runs",
        project="demo",
        experiment_name="exp",
        batch_name="b1",
    )
    batch_root = identity.batch_root
    plan = sample_run_plan(
        run_index=1,
        total_runs=2,
        run_id="r1",
        run_dir=str(batch_root / "runs" / "run_001_r1"),
        run_dir_rel="runs/run_001_r1",
        train_stage=sample_stage_plan(workdir=tmp_path),
    )
    plan2 = sample_run_plan(
        run_index=2,
        total_runs=2,
        run_id="r2",
        run_dir=str(batch_root / "runs" / "run_002_r2"),
        run_dir_rel="runs/run_002_r2",
        train_stage=sample_stage_plan(workdir=tmp_path),
    )
    snap1 = sample_run_snapshot(run_index=1, total_runs=2, run_id="r1",
                                 project="demo", experiment_name="exp",
                                 model_name=plan.model_name, train_mode=plan.train_mode)
    snap2 = sample_run_snapshot(run_index=2, total_runs=2, run_id="r2",
                                 project="demo", experiment_name="exp",
                                 model_name=plan2.model_name, train_mode=plan2.train_mode)
    return PlannedBatch(
        identity=identity,
        planned_runs=(PlannedRun(plan=plan, snapshot=snap1), PlannedRun(plan=plan2, snapshot=snap2)),
        storage_config=storage_config,
    )


class EngineNoneE2ETests(unittest.TestCase):
    def test_filesystem_full_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(tmp_path)
            batch_root = planned_batch.batch_root

            # Generate
            env = make_template_env()
            store = create_planning_store(planned_batch.storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)
            self.assertTrue(batch_root.exists())

            # Descriptor written
            descriptor = read_storage_descriptor(batch_root)
            self.assertIsNotNone(descriptor)
            self.assertEqual(descriptor.backend.engine, "none")

            # storage.json on disk
            self.assertTrue((batch_root / "meta" / "storage.json").exists())

            # Load plan for array task
            read_store = create_planning_store_for_read(batch_root)
            plan = read_store.load_plan_for_array_task(batch_root, group_index=1, task_index=0)
            self.assertIsNotNone(plan)
            self.assertEqual(plan.run_id, "r1")

            # Load snapshot
            snapshot = read_store.load_run_snapshot(batch_root, "r2")
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot.run_id, "r2")

            # Status via handle — all runs visible, none attempted
            handle = open_batch_storage(batch_root)
            views = handle.list_batch_run_views()
            self.assertEqual(len(views), 2)
            self.assertIsNone(views[0].latest_status)
            self.assertIsNone(views[1].latest_status)
            self.assertEqual(views[0].run_id, "r1")
            self.assertEqual(views[1].run_id, "r2")

            # Planning files exist
            self.assertTrue((batch_root / "meta" / "runs_manifest.jsonl").exists())
            self.assertTrue((batch_root / "records").exists())


class EngineSqliteRecoveryE2ETests(unittest.TestCase):
    def test_sqlite_with_recovery_full_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(
                tmp_path,
                {"backend": {"engine": "sqlite"}, "exports": {"planning_recovery": True}},
            )
            batch_root = planned_batch.batch_root

            # Generate
            env = make_template_env()
            store = create_planning_store(planned_batch.storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)
            self.assertTrue(batch_root.exists())

            # DB exists
            db_path = db_path_for_batch(batch_root, planned_batch.storage_config)
            self.assertTrue(db_path.exists())

            # Descriptor
            descriptor = read_storage_descriptor(batch_root)
            self.assertIsNotNone(descriptor)
            self.assertEqual(descriptor.backend.engine, "sqlite")

            # Recovery files exist
            self.assertTrue((batch_root / "meta" / "runs_manifest.jsonl").exists())
            self.assertTrue((batch_root / "records").exists())
            self.assertTrue(
                (batch_root / "runs" / "run_001_r1" / "meta" / "run_snapshot.json").exists()
            )

            # Load from DB (even after deleting recovery files)
            import shutil
            shutil.rmtree(batch_root / "records")
            (batch_root / "meta" / "runs_manifest.jsonl").unlink()

            read_store = create_planning_store_for_read(batch_root)
            plans = read_store.load_batch_run_plans(batch_root)
            self.assertEqual(len(plans), 2)

            plan = read_store.load_plan_for_array_task(batch_root, group_index=1, task_index=0)
            self.assertIsNotNone(plan)
            self.assertEqual(plan.run_id, "r1")

            snapshot = read_store.load_run_snapshot(batch_root, "r2")
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot.run_id, "r2")

            # Status via handle
            handle = open_batch_storage(batch_root)
            views = handle.list_batch_run_views()
            self.assertEqual(len(views), 2)


class EngineSqlitePureDBE2ETests(unittest.TestCase):
    def test_sqlite_pure_db_no_recovery_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(
                tmp_path,
                {"backend": {"engine": "sqlite"}, "exports": {"planning_recovery": False}},
            )
            batch_root = planned_batch.batch_root

            # Generate
            env = make_template_env()
            store = create_planning_store(planned_batch.storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)
            self.assertTrue(batch_root.exists())

            # DB exists
            db_path = db_path_for_batch(batch_root, planned_batch.storage_config)
            self.assertTrue(db_path.exists())

            # No planning files on disk in pure DB mode
            self.assertFalse((batch_root / "records").exists())
            self.assertFalse((batch_root / "meta" / "runs_manifest.jsonl").exists())
            self.assertFalse(
                (batch_root / "runs" / "run_001_r1" / "meta" / "run_snapshot.json").exists()
            )
            self.assertFalse(
                (batch_root / "runs" / "run_001_r1" / "resolved_config.yaml").exists()
            )

            # Load from DB — still works
            read_store = create_planning_store_for_read(batch_root)
            plans = read_store.load_batch_run_plans(batch_root)
            self.assertEqual(len(plans), 2)

            plan = read_store.load_plan_for_array_task(batch_root, group_index=1, task_index=0)
            self.assertIsNotNone(plan)

            snapshot = read_store.load_run_snapshot(batch_root, "r1")
            self.assertIsNotNone(snapshot)

            # Status via handle
            handle = open_batch_storage(batch_root)
            views = handle.list_batch_run_views()
            self.assertEqual(len(views), 2)
            self.assertIsNone(views[0].latest_status)

    def test_sqlite_pure_db_loaded_plans_do_not_expose_dangling_record_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(
                tmp_path,
                {"backend": {"engine": "sqlite"}, "exports": {"planning_recovery": False}},
            )
            batch_root = planned_batch.batch_root

            env = make_template_env()
            store = create_planning_store(planned_batch.storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)

            read_store = create_planning_store_for_read(batch_root)
            plans = read_store.load_batch_run_plans(batch_root)
            plan = read_store.load_plan_for_array_task(batch_root, group_index=1, task_index=0)

            self.assertEqual(len(plans), 2)
            self.assertIsNotNone(plan)
            self.assertIsNone(plans[0].dispatch.record_path)
            self.assertIsNone(plans[0].dispatch.record_path_rel)
            self.assertIsNone(plan.dispatch.record_path)
            self.assertIsNone(plan.dispatch.record_path_rel)
            self.assertFalse((batch_root / "records").exists())


class CustomSqlitePathE2ETests(unittest.TestCase):
    def test_custom_sqlite_path_writes_and_reads_from_configured_location(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(
                tmp_path,
                {"backend": {"engine": "sqlite", "sqlite": {"path": "data/custom.sqlite3"}}},
            )
            batch_root = planned_batch.batch_root

            env = make_template_env()
            store = create_planning_store(planned_batch.storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)

            # DB at custom path, NOT at default location
            self.assertTrue((batch_root / "data" / "custom.sqlite3").exists())
            self.assertFalse((batch_root / "meta" / "slurmforge.sqlite3").exists())

            # Read via auto-detect (reads storage.json → finds custom path)
            read_store = create_planning_store_for_read(batch_root)
            plans = read_store.load_batch_run_plans(batch_root)
            self.assertEqual(len(plans), 2)
            self.assertEqual(plans[0].run_id, "r1")

            snapshot = read_store.load_run_snapshot(batch_root, "r2")
            self.assertIsNotNone(snapshot)

    def test_descriptor_prioritizes_custom_sqlite_path_over_default_guess(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(
                tmp_path,
                {
                    "backend": {"engine": "sqlite", "sqlite": {"path": "data/custom.sqlite3"}},
                    "exports": {"planning_recovery": False},
                },
            )
            batch_root = planned_batch.batch_root

            env = make_template_env()
            store = create_planning_store(planned_batch.storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)

            custom_db = batch_root / "data" / "custom.sqlite3"
            default_db = batch_root / "meta" / "slurmforge.sqlite3"

            self.assertTrue(custom_db.exists())
            self.assertFalse(default_db.exists())

            # Create a misleading default-path DB file. Reads must still use the
            # descriptor's configured custom path.
            default_db.parent.mkdir(parents=True, exist_ok=True)
            default_db.write_text("", encoding="utf-8")

            read_store = create_planning_store_for_read(batch_root)
            plans = read_store.load_batch_run_plans(batch_root)
            snapshot = read_store.load_run_snapshot(batch_root, "r1")

            self.assertEqual(len(plans), 2)
            self.assertEqual(plans[0].run_id, "r1")
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot.run_id, "r1")

            handle = open_batch_storage(batch_root)
            views = handle.list_batch_run_views()
            self.assertEqual(len(views), 2)
            self.assertEqual([view.run_id for view in views], ["r1", "r2"])


class PureDBReconcileE2ETests(unittest.TestCase):
    def test_sqlite_pure_db_reconcile_works_without_planning_files(self) -> None:
        """Critical test: planning_recovery=false + execution → reconcile → status."""
        from slurmforge.pipeline.status import ExecutionStatus, write_execution_status, status_path_for_result_dir
        from slurmforge.pipeline.status.store import write_latest_result_dir
        from slurmforge.storage.backends.sqlite import SqliteExecutionStore

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch = _build_planned_batch(
                tmp_path,
                {"backend": {"engine": "sqlite"}, "exports": {"planning_recovery": False}},
            )
            batch_root = planned_batch.batch_root
            storage_config = planned_batch.storage_config

            # Generate (pure DB, no recovery files)
            env = make_template_env()
            store = create_planning_store(storage_config, env)
            materialize_batch(planned_batch=planned_batch, planning_store=store)

            # Verify no planning files
            self.assertFalse((batch_root / "records").exists())
            self.assertFalse((batch_root / "meta" / "runs_manifest.jsonl").exists())

            # Simulate execution: write runtime journal files for run r1
            run_dir = batch_root / "runs" / "run_001_r1"
            result_dir = run_dir / "job-99999"
            meta_dir = result_dir / "meta"
            meta_dir.mkdir(parents=True, exist_ok=True)
            write_latest_result_dir(run_dir, result_dir)
            status = ExecutionStatus(
                state="success",
                slurm_state="COMPLETED",
                reason="ok",
                shell_exit_code=0,
                job_key="99999",
                slurm_job_id="99999",
                started_at="2026-04-13T00:00:00+00:00",
                finished_at="2026-04-13T00:05:00+00:00",
                result_dir=str(result_dir),
            )
            write_execution_status(status_path_for_result_dir(result_dir), status)

            # Reconcile — must work without planning files (reads run list from DB)
            exec_store = SqliteExecutionStore(storage_config)
            exec_store.reconcile_batch(batch_root)

            # Verify DB has the attempt
            from slurmforge.storage.backends.sqlite.connection import open_batch_db
            db_path = db_path_for_batch(batch_root, storage_config)
            with open_batch_db(db_path, config=storage_config) as conn:
                count = conn.execute("SELECT COUNT(*) AS n FROM attempts").fetchone()["n"]
                state = conn.execute("SELECT state FROM attempts WHERE run_id='r1'").fetchone()
            self.assertEqual(count, 1)
            self.assertEqual(state["state"], "success")

            # Status via handle — shows r1 as success, r2 as unattempted
            handle = open_batch_storage(batch_root)
            views = handle.list_batch_run_views()
            self.assertEqual(len(views), 2)

            r1_view = next(v for v in views if v.run_id == "r1")
            r2_view = next(v for v in views if v.run_id == "r2")
            self.assertIsNotNone(r1_view.latest_status)
            self.assertEqual(r1_view.latest_status.state, "success")
            self.assertIsNone(r2_view.latest_status)


class StorageDescriptorTests(unittest.TestCase):
    def test_descriptor_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = normalize_storage_config({"backend": {"engine": "sqlite"}})
            from slurmforge.storage.descriptor import write_storage_descriptor
            write_storage_descriptor(tmp_path, config, tmp_path / "batch_root")
            roundtripped = read_storage_descriptor(tmp_path / ".." / tmp_path.name)
            self.assertIsNotNone(roundtripped)
            self.assertEqual(roundtripped.backend.engine, "sqlite")

    def test_sqlite_path_escape_rejected(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.pipeline.config.codecs import normalize_storage_config
        with self.assertRaises(ConfigContractError):
            normalize_storage_config({"backend": {"engine": "sqlite", "sqlite": {"path": "/absolute/path.db"}}})

    def test_sqlite_path_parent_escape_rejected(self) -> None:
        from slurmforge.storage.backends.sqlite.connection import db_path_for_batch
        config = normalize_storage_config({"backend": {"engine": "sqlite", "sqlite": {"path": "../../escape.db"}}})
        with self.assertRaises(ConfigContractError):
            db_path_for_batch(Path("/tmp/batch_root"), config)

    def test_missing_descriptor_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "batch"
            (batch_root / "meta").mkdir(parents=True, exist_ok=True)

            with self.assertRaisesRegex(ConfigContractError, "No storage descriptor found"):
                storage_config_for_batch(batch_root)

    def test_corrupt_descriptor_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "batch"
            meta_dir = batch_root / "meta"
            meta_dir.mkdir(parents=True, exist_ok=True)
            (meta_dir / "storage.json").write_text("{not-json", encoding="utf-8")

            with self.assertRaisesRegex(ConfigContractError, "Corrupt storage descriptor"):
                storage_config_for_batch(batch_root)
