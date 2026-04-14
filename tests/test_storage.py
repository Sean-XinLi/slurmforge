from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from slurmforge.errors import ConfigContractError
from slurmforge.pipeline.config.codecs import normalize_storage_config
from slurmforge.pipeline.materialization import materialize_batch
from slurmforge.pipeline.planning import BatchIdentity, PlannedBatch, PlannedRun
from slurmforge.pipeline.status import (
    ExecutionStatus,
    status_path_for_result_dir,
    write_execution_status,
    write_latest_result_dir,
)
from slurmforge.pipeline.checkpoints import CheckpointState, write_checkpoint_state
from slurmforge.pipeline.train_outputs.cache import write_train_outputs_manifest
from slurmforge.pipeline.train_outputs.models import TrainOutputsManifest
from slurmforge.pipeline.train_outputs.paths import train_outputs_manifest_path_for_result_dir
from slurmforge.storage import create_planning_store_for_read
from slurmforge.storage.backends.sqlite import SqliteExecutionStore, SqlitePlanningStore
from slurmforge.storage.backends.sqlite.connection import db_path_for_batch, open_batch_db
from tests._support import make_template_env, sample_run_plan, sample_run_snapshot, sample_stage_plan


def _build_sqlite_planned_batch(tmp_path: Path) -> tuple[PlannedBatch, object]:
    storage_config = normalize_storage_config({"backend": {"engine": "sqlite"}})
    identity = BatchIdentity(
        project_root=tmp_path,
        base_output_dir=tmp_path / "runs",
        project="demo",
        experiment_name="exp",
        batch_name="sqlite",
    )
    batch_root = identity.batch_root
    plan = sample_run_plan(
        run_index=1,
        total_runs=1,
        run_id="r1",
        run_dir=str(batch_root / "runs" / "run_001_r1"),
        run_dir_rel="runs/run_001_r1",
        train_stage=sample_stage_plan(workdir=tmp_path),
    )
    snapshot = sample_run_snapshot(
        run_index=plan.run_index,
        total_runs=plan.total_runs,
        run_id=plan.run_id,
        project=plan.project,
        experiment_name=plan.experiment_name,
        model_name=plan.model_name,
        train_mode=plan.train_mode,
    )
    planned_batch = PlannedBatch(
        identity=identity,
        planned_runs=(PlannedRun(plan=plan, snapshot=snapshot),),
        storage_config=storage_config,
    )
    return planned_batch, storage_config


class StorageConfigTests(unittest.TestCase):
    def test_normalize_storage_config_defaults_to_filesystem(self) -> None:
        config = normalize_storage_config(None)

        self.assertEqual(config.backend.engine, "none")
        self.assertEqual(config.backend.sqlite.path, "auto")
        self.assertEqual(config.backend.sqlite.options.journal_mode, "DELETE")
        self.assertTrue(config.exports.planning_recovery)

    def test_normalize_storage_config_rejects_empty_sqlite_path(self) -> None:
        with self.assertRaisesRegex(ConfigContractError, "empty string"):
            normalize_storage_config(
                {
                    "backend": {
                        "engine": "sqlite",
                        "sqlite": {"path": ""},
                    }
                }
            )

    def test_normalize_storage_config_rejects_invalid_journal_mode(self) -> None:
        with self.assertRaisesRegex(ConfigContractError, "journal_mode"):
            normalize_storage_config(
                {
                    "backend": {
                        "engine": "sqlite",
                        "sqlite": {"options": {"journal_mode": "WAL"}},
                    }
                }
            )


class SqlitePlanningStoreTests(unittest.TestCase):
    def test_sqlite_materialization_loads_plan_and_snapshot_from_db_without_planning_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch, storage_config = _build_sqlite_planned_batch(tmp_path)
            batch_root = planned_batch.batch_root

            materialize_batch(
                planned_batch=planned_batch,
                planning_store=SqlitePlanningStore(make_template_env(), storage_config=storage_config),
            )

            db_path = db_path_for_batch(batch_root, storage_config)
            self.assertTrue(db_path.exists())

            shutil.rmtree(batch_root / "records")
            (batch_root / "meta" / "runs_manifest.jsonl").unlink()
            (batch_root / "runs" / "run_001_r1" / "meta" / "run_snapshot.json").unlink()

            store = create_planning_store_for_read(batch_root)
            plan = store.load_plan_for_array_task(batch_root, group_index=1, task_index=0)
            snapshot = store.load_run_snapshot(batch_root, "r1")
            plans = store.load_batch_run_plans(batch_root)

            self.assertIsNotNone(plan)
            self.assertEqual(plan.run_id, "r1")
            self.assertEqual(plan.dispatch.array_group, 1)
            self.assertEqual(plan.dispatch.array_task_index, 0)
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot.run_id, "r1")
            self.assertEqual(len(plans), 1)
            self.assertEqual(plans[0].run_id, "r1")


class SqliteExecutionStoreTests(unittest.TestCase):
    def test_sqlite_execution_store_reconciles_runtime_metadata_from_meta_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            planned_batch, storage_config = _build_sqlite_planned_batch(tmp_path)
            batch_root = planned_batch.batch_root

            materialize_batch(
                planned_batch=planned_batch,
                planning_store=SqlitePlanningStore(make_template_env(), storage_config=storage_config),
            )

            run_dir = batch_root / "runs" / "run_001_r1"
            result_dir = run_dir / "job-12345"
            meta_dir = result_dir / "meta"
            meta_dir.mkdir(parents=True, exist_ok=True)
            write_latest_result_dir(run_dir, result_dir)

            status = ExecutionStatus(
                state="success",
                slurm_state="COMPLETED",
                reason="train/eval completed successfully",
                shell_exit_code=0,
                job_key="12345",
                slurm_job_id="12345",
                started_at="2026-04-13T00:00:00+00:00",
                finished_at="2026-04-13T00:10:00+00:00",
                result_dir=str(result_dir),
            )
            write_execution_status(status_path_for_result_dir(result_dir), status)
            write_checkpoint_state(
                result_dir,
                CheckpointState(
                    latest_checkpoint_rel="checkpoints/step-1.ckpt",
                    selection_reason="max_step",
                    global_step=1,
                ),
            )
            write_train_outputs_manifest(
                train_outputs_manifest_path_for_result_dir(result_dir),
                TrainOutputsManifest(
                    run_id="r1",
                    model_name="convbert",
                    result_dir=str(result_dir),
                    checkpoint_dir=str(result_dir / "checkpoints"),
                    primary_policy="latest",
                    primary_checkpoint=str(result_dir / "checkpoints" / "step-1.ckpt"),
                    latest_checkpoint=str(result_dir / "checkpoints" / "step-1.ckpt"),
                    selection_reason="max_step",
                    status="ok",
                ),
            )
            (meta_dir / "artifact_manifest.json").write_text(
                json.dumps(
                    {
                        "status": "ok",
                        "failure_count": 0,
                        "workdirs": [str(tmp_path / "workdir")],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            store = SqliteExecutionStore(storage_config)
            store.reconcile_batch(batch_root)
            attempts = store.load_latest_attempts(batch_root, {"r1": run_dir})

            self.assertIn("r1", attempts)
            self.assertEqual(attempts["r1"].latest_status.state, "success")
            self.assertEqual(attempts["r1"].latest_result_dir, str(result_dir.resolve()))

            with open_batch_db(db_path_for_batch(batch_root, storage_config), config=storage_config) as conn:
                attempt_count = conn.execute("SELECT COUNT(*) AS n FROM attempts").fetchone()["n"]
                row = conn.execute("SELECT * FROM attempts WHERE run_id = 'r1'").fetchone()

            self.assertEqual(attempt_count, 1)
            self.assertEqual(row["state"], "success")
            self.assertEqual(row["latest_checkpoint"], "checkpoints/step-1.ckpt")
            self.assertEqual(row["train_outputs_status"], "ok")
            self.assertEqual(row["artifact_status"], "ok")
            self.assertIsNotNone(row["status_json"])
            self.assertIsNotNone(row["checkpoint_json"])
            self.assertIsNotNone(row["train_outputs_json"])
            self.assertIsNotNone(row["artifact_json"])

            store.reconcile_batch(batch_root)
            with open_batch_db(db_path_for_batch(batch_root, storage_config), config=storage_config) as conn:
                attempt_count_after = conn.execute("SELECT COUNT(*) AS n FROM attempts").fetchone()["n"]

            self.assertEqual(attempt_count_after, 1)
