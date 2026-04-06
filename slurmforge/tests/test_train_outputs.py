from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from slurmforge.execution import write_train_outputs as write_train_outputs_cli
from slurmforge.pipeline.train_outputs import (
    build_train_outputs_manifest,
    load_or_build_train_outputs_manifest,
    read_train_outputs_manifest,
    write_train_outputs_contract,
)


class TrainOutputsTests(unittest.TestCase):
    def test_write_train_outputs_contract_persists_manifest_and_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "job-123"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            checkpoint = checkpoint_dir / "step-5.ckpt"
            checkpoint.write_text("checkpoint", encoding="utf-8")

            manifest_path = result_dir / "meta" / "train_outputs.json"
            env_path = result_dir / "meta" / "train_outputs.env"
            manifest = write_train_outputs_contract(
                result_dir=result_dir,
                manifest_path=manifest_path,
                env_path=env_path,
                checkpoint_globs=[],
                run_id="r1",
                model_name="demo-model",
            )

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            env_text = env_path.read_text(encoding="utf-8")

        self.assertEqual(manifest.status, "ok")
        self.assertEqual(payload["primary_checkpoint"], str(checkpoint.resolve()))
        self.assertIn("AI_INFRA_TRAIN_ARTIFACT_MANIFEST=", env_text)
        self.assertIn(str(checkpoint.resolve()), env_text)

    def test_build_train_outputs_manifest_collects_checkpoints_from_workdir_before_eval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "job-123"
            workdir = tmp_path / "workdir"
            source_checkpoint = workdir / "outputs" / "step-9.ckpt"
            source_checkpoint.parent.mkdir(parents=True, exist_ok=True)
            source_checkpoint.write_text("checkpoint", encoding="utf-8")

            manifest = build_train_outputs_manifest(
                result_dir=result_dir,
                checkpoint_globs=["outputs/**/*.ckpt"],
                run_id="r1",
                model_name="demo-model",
                workdirs=[workdir],
            )

            copied_checkpoint = result_dir / "checkpoints" / "outputs" / "step-9.ckpt"
            self.assertEqual(manifest.status, "ok")
            self.assertTrue(copied_checkpoint.exists())
            self.assertEqual(manifest.primary_checkpoint, str(copied_checkpoint.resolve()))

    def test_build_train_outputs_manifest_prefers_best_named_checkpoint_for_best_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "job-123"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            (checkpoint_dir / "step-5.ckpt").write_text("latest", encoding="utf-8")
            best = checkpoint_dir / "best-step-3.ckpt"
            best.write_text("best", encoding="utf-8")

            manifest = build_train_outputs_manifest(
                result_dir=result_dir,
                checkpoint_globs=[],
                run_id="r1",
                model_name="demo-model",
                primary_policy="best",
            )

        self.assertEqual(manifest.status, "ok")
        self.assertEqual(manifest.best_checkpoint, str(best.resolve()))
        self.assertEqual(manifest.primary_checkpoint, str(best.resolve()))

    def test_build_train_outputs_manifest_supports_explicit_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "job-123"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            explicit = checkpoint_dir / "named.ckpt"
            explicit.write_text("checkpoint", encoding="utf-8")

            manifest = build_train_outputs_manifest(
                result_dir=result_dir,
                checkpoint_globs=[],
                run_id="r1",
                model_name="demo-model",
                primary_policy="explicit",
                explicit_checkpoint="named.ckpt",
            )

        self.assertEqual(manifest.status, "ok")
        self.assertEqual(manifest.primary_checkpoint, str(explicit.resolve()))
        self.assertEqual(manifest.selection_reason, "explicit_checkpoint")

    def test_write_train_outputs_helper_requires_primary_checkpoint_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "job-123"
            manifest_path = result_dir / "meta" / "train_outputs.json"
            env_path = result_dir / "meta" / "train_outputs.env"

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
                    "--require_primary",
                ]
            )

            manifest = read_train_outputs_manifest(result_dir)

        self.assertEqual(exit_code, 2)
        self.assertIsNotNone(manifest)
        self.assertEqual(manifest.status, "no_checkpoint")
        self.assertEqual(manifest.primary_checkpoint, "")

    def test_load_or_build_train_outputs_manifest_recomputes_stale_empty_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result_dir = tmp_path / "job-123"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            checkpoint = checkpoint_dir / "step-12.ckpt"
            checkpoint.write_text("checkpoint", encoding="utf-8")
            write_train_outputs_contract(
                result_dir=result_dir,
                manifest_path=result_dir / "meta" / "train_outputs.json",
                env_path=result_dir / "meta" / "train_outputs.env",
                checkpoint_globs=[],
                run_id="r1",
                model_name="demo-model",
            )
            stale_manifest_path = result_dir / "meta" / "train_outputs.json"
            payload = json.loads(stale_manifest_path.read_text(encoding="utf-8"))
            payload["primary_checkpoint"] = ""
            payload["status"] = "no_checkpoint"
            stale_manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

            manifest = load_or_build_train_outputs_manifest(
                result_dir=result_dir,
                checkpoint_globs=[],
                run_id="r1",
                model_name="demo-model",
            )

        self.assertEqual(manifest.status, "ok")
        self.assertEqual(manifest.primary_checkpoint, str(checkpoint.resolve()))
