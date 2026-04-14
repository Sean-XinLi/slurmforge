from __future__ import annotations

import os
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from slurmforge.execution import write_attempt_result as write_attempt_result_cli
from slurmforge.execution import write_train_outputs as write_train_outputs_cli
from slurmforge.execution.artifacts import cli as artifact_cli
from slurmforge.pipeline.config.codecs import normalize_storage_config
from slurmforge.pipeline.materialization.blocks.env_setup import append_env_setup
from slurmforge.pipeline.status import (
    build_attempt_result,
    read_execution_status,
    read_latest_result_dir,
    status_path_for_result_dir,
)
from slurmforge.pipeline.train_outputs.models import TrainOutputsManifest
from slurmforge.storage.backends.filesystem import FileSystemExecutionStore
from slurmforge.storage.lifecycle import ExecutionLifecycle
from tests._support import sample_run_plan, sample_stage_plan


class ExecutionLifecycleTests(unittest.TestCase):
    def test_begin_and_finalize_attempt_write_latest_pointer_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            run_dir = tmp_path / "batch" / "runs" / "run_001_r1"

            store = FileSystemExecutionStore()
            lifecycle = ExecutionLifecycle(store)
            env = {
                "SLURM_JOB_ID": "12345",
                "SLURM_ARRAY_JOB_ID": "12345",
                "SLURM_ARRAY_TASK_ID": "0",
            }

            result_dir, initial_status = lifecycle.begin_attempt(run_dir, env=env)
            attempt = build_attempt_result(
                result_dir=result_dir,
                train_exit_code=0,
                eval_exit_code=0,
                env=env,
            )
            store.write_attempt_result(result_dir, attempt)
            final_status = lifecycle.finalize_attempt(
                result_dir,
                started_at=initial_status.started_at,
                shell_exit_code=0,
            )

            latest_result_dir = read_latest_result_dir(run_dir)
            persisted_status = read_execution_status(status_path_for_result_dir(result_dir))

        self.assertEqual(result_dir, run_dir / "job-12345")
        self.assertIsNotNone(latest_result_dir)
        self.assertEqual(latest_result_dir, result_dir.resolve())
        self.assertEqual(initial_status.state, "running")
        self.assertEqual(final_status.state, "success")
        self.assertIsNotNone(persisted_status)
        self.assertEqual(persisted_status.state, "success")
        self.assertEqual(persisted_status.job_key, "12345")


class EnvSetupTests(unittest.TestCase):
    def test_append_env_setup_exports_blank_planning_paths_when_recovery_disabled(self) -> None:
        plan = sample_run_plan(
            run_dir="/tmp/batch/runs/run_001_r1",
            run_dir_rel="runs/run_001_r1",
            train_stage=sample_stage_plan(workdir=Path("/tmp").resolve()),
        )
        storage_config = normalize_storage_config(
            {"backend": {"engine": "sqlite"}, "exports": {"planning_recovery": False}}
        )

        lines: list[str] = []
        append_env_setup(lines, plan, storage_config=storage_config)
        text = "\n".join(lines)

        self.assertIn("export AI_INFRA_EXECUTION_PLAN_JSON_PATH=''", text)
        self.assertIn("export AI_INFRA_RUN_SNAPSHOT_JSON_PATH=''", text)
        self.assertIn("export AI_INFRA_RESOLVED_CONFIG_YAML_PATH=/tmp/batch/runs/run_001_r1/resolved_config.yaml", text)


class RuntimeHelperStorageRoutingTests(unittest.TestCase):
    def test_write_attempt_result_helper_requires_batch_root_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "run" / "job-100"

            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "AI_INFRA_BATCH_ROOT"):
                    write_attempt_result_cli.write_attempt_result_for_result_dir(
                        result_dir=result_dir,
                        train_exit_code=0,
                        eval_exit_code=0,
                        env={"SLURM_JOB_ID": "100"},
                    )

    def test_write_attempt_result_helper_uses_storage_when_batch_root_env_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "run" / "job-101"
            execution = MagicMock()
            handle = SimpleNamespace(execution=execution)

            with patch.dict(os.environ, {"AI_INFRA_BATCH_ROOT": str(tmp_path / "batch")}, clear=False):
                with patch("slurmforge.storage.open_batch_storage", return_value=handle) as open_mock:
                    path = write_attempt_result_cli.write_attempt_result_for_result_dir(
                        result_dir=result_dir,
                        train_exit_code=0,
                        eval_exit_code=0,
                        env={"SLURM_JOB_ID": "101"},
                    )

        open_mock.assert_called_once()
        execution.write_attempt_result.assert_called_once()
        call_result_dir, attempt = execution.write_attempt_result.call_args.args
        self.assertEqual(call_result_dir, result_dir.resolve())
        self.assertEqual(attempt.train_exit_code, 0)
        self.assertEqual(attempt.eval_exit_code, 0)
        self.assertEqual(path, result_dir.resolve() / "meta" / "attempt_result.json")

    def test_write_train_outputs_helper_uses_storage_when_batch_root_env_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "run" / "job-201"
            manifest_path = result_dir / "meta" / "train_outputs.json"
            env_path = result_dir / "meta" / "train_outputs.env"
            manifest = TrainOutputsManifest(
                run_id="r1",
                model_name="demo-model",
                result_dir=str(result_dir),
                checkpoint_dir=str(result_dir / "checkpoints"),
                status="ok",
                primary_checkpoint=str(result_dir / "checkpoints" / "step-1.ckpt"),
            )
            execution = MagicMock()
            handle = SimpleNamespace(execution=execution)

            with patch.dict(os.environ, {"AI_INFRA_BATCH_ROOT": str(tmp_path / "batch")}, clear=False):
                with patch(
                    "slurmforge.execution.write_train_outputs.write_train_outputs_contract",
                    return_value=manifest,
                ) as contract_mock:
                    with patch("slurmforge.storage.open_batch_storage", return_value=handle) as open_mock:
                        exit_code = write_train_outputs_cli.main(
                            [
                                "--result_dir",
                                str(result_dir),
                                "--manifest_path",
                                str(manifest_path),
                                "--env_path",
                                str(env_path),
                                "--run_id",
                                "r1",
                                "--model_name",
                                "demo-model",
                            ]
                        )

        self.assertEqual(exit_code, 0)
        contract_mock.assert_called_once()
        open_mock.assert_called_once()
        execution.write_train_outputs_manifest.assert_called_once_with(result_dir.resolve(), manifest)

    def test_write_train_outputs_helper_allows_manual_use_without_batch_root_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "run" / "job-202"
            manifest_path = result_dir / "meta" / "train_outputs.json"
            env_path = result_dir / "meta" / "train_outputs.env"
            manifest = TrainOutputsManifest(
                run_id="r2",
                model_name="demo-model",
                result_dir=str(result_dir),
                checkpoint_dir=str(result_dir / "checkpoints"),
                status="ok",
                primary_checkpoint=str(result_dir / "checkpoints" / "step-1.ckpt"),
            )

            with patch.dict(os.environ, {}, clear=True):
                with patch(
                    "slurmforge.execution.write_train_outputs.write_train_outputs_contract",
                    return_value=manifest,
                ) as contract_mock:
                    with patch("slurmforge.storage.open_batch_storage") as open_mock:
                        exit_code = write_train_outputs_cli.main(
                            [
                                "--result_dir",
                                str(result_dir),
                                "--manifest_path",
                                str(manifest_path),
                                "--env_path",
                                str(env_path),
                                "--run_id",
                                "r2",
                                "--model_name",
                                "demo-model",
                            ]
                        )

        self.assertEqual(exit_code, 0)
        contract_mock.assert_called_once()
        open_mock.assert_not_called()

    def test_artifact_sync_cli_uses_storage_when_batch_root_env_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "run" / "job-301"
            summary = {
                "status": "ok",
                "failure_count": 0,
                "workdirs": [str((tmp_path / "work").resolve())],
                "copied": {},
                "failures": {},
            }
            execution = MagicMock()
            handle = SimpleNamespace(execution=execution)
            args = Namespace(
                workdir=[str(tmp_path / "work")],
                result_dir=str(result_dir),
                checkpoint_glob=[],
                eval_csv_glob=[],
                eval_image_glob=[],
                extra_glob=[],
                max_matches_per_glob=50,
            )

            with patch.dict(os.environ, {"AI_INFRA_BATCH_ROOT": str(tmp_path / "batch")}, clear=False):
                with patch("slurmforge.execution.artifacts.cli.parse_args", return_value=args):
                    with patch("slurmforge.execution.artifacts.cli.sync_artifacts", return_value=summary):
                        with patch("slurmforge.storage.open_batch_storage", return_value=handle) as open_mock:
                            artifact_cli.main()

        open_mock.assert_called_once()
        execution.write_artifact_manifest.assert_called_once_with(result_dir.resolve(), summary)

    def test_artifact_sync_cli_allows_manual_use_without_batch_root_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "run" / "job-302"
            summary = {
                "status": "ok",
                "failure_count": 0,
                "workdirs": [str((tmp_path / "work").resolve())],
                "copied": {},
                "failures": {},
            }
            args = Namespace(
                workdir=[str(tmp_path / "work")],
                result_dir=str(result_dir),
                checkpoint_glob=[],
                eval_csv_glob=[],
                eval_image_glob=[],
                extra_glob=[],
                max_matches_per_glob=50,
            )

            with patch.dict(os.environ, {}, clear=True):
                with patch("slurmforge.execution.artifacts.cli.parse_args", return_value=args):
                    with patch("slurmforge.execution.artifacts.cli.sync_artifacts", return_value=summary) as sync_mock:
                        with patch("slurmforge.storage.open_batch_storage") as open_mock:
                            artifact_cli.main()

        sync_mock.assert_called_once()
        open_mock.assert_not_called()
