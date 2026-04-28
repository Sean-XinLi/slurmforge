from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    prepare_stage_submission,
    upstream_bindings_from_train_batch,
    write_demo_project,
)
from tests.support.internal_records import write_stage_batch_layout
import json
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch


class ExecutorTests(StageBatchSystemTestCase):
    def test_executor_runs_before_steps_with_stage_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["before"] = [
                {
                    "name": "prepare-marker",
                    "run": "printf '%s' \"$DEMO_ENV\" > before_marker.txt",
                },
            ]
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "assert Path('before_marker.txt').read_text() == '1'",
                        "Path('checkpoints').mkdir(exist_ok=True)",
                        "(Path('checkpoints') / 'step_1.pt').write_text('ckpt')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )

            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            attempt_dir = train_run_dir / "attempts" / "0001"
            self.assertTrue((root / "before_marker.txt").exists())
            self.assertTrue((attempt_dir / "environment_plan.json").exists())
            self.assertTrue((attempt_dir / "before_steps.json").exists())

    def test_executor_runtime_contract_failure_blocks_user_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runtime"]["user"]["default"]["python"]["min_version"] = "99.0"
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "(Path('user_script_started.txt')).write_text('started')",
                        "Path('checkpoints').mkdir(exist_ok=True)",
                        "(Path('checkpoints') / 'step_1.pt').write_text('ckpt')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

            self.assertNotEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )

            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            status = json.loads((train_run_dir / "status.json").read_text())
            probe = json.loads(
                (train_run_dir / "attempts" / "0001" / "runtime_probe.json").read_text()
            )
            probe_states = {
                item["runtime_role"]: item["state"] for item in probe["probes"]
            }
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "runtime_contract_error")
            self.assertEqual(probe_states["user"], "failed")
            self.assertFalse((root / "user_script_started.txt").exists())

    def test_executor_unknown_error_writes_traceback_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

            with patch(
                "slurmforge.executor.runner.build_shell_script",
                side_effect=RuntimeError("executor boom"),
            ):
                self.assertNotEqual(
                    execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
                )

            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            diagnostic = (
                train_run_dir / "attempts" / "0001" / "logs" / "executor_traceback.log"
            )
            self.assertTrue(diagnostic.exists())
            text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: executor boom", text)
            self.assertIn("Traceback", text)

    def test_executor_preflight_missing_input_fails_before_user_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            runs, bindings = upstream_bindings_from_train_batch(
                spec, Path(train_batch.submission_root)
            )
            checkpoint_path = Path(bindings[runs[0].run_id][0].resolved.path)
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)
            checkpoint_path.unlink()

            self.assertNotEqual(
                execute_stage_task(Path(eval_batch.submission_root), 1, 0), 0
            )

            eval_run_dir = (
                Path(eval_batch.submission_root)
                / eval_batch.stage_instances[0].run_dir_rel
            )
            status = json.loads((eval_run_dir / "status.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "input_contract_error")
            self.assertTrue((eval_run_dir / "input_verification.json").exists())
            self.assertFalse((root / "eval").exists())

    def test_submit_preflight_fails_on_expected_input_digest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            runs, bindings = upstream_bindings_from_train_batch(
                spec, Path(train_batch.submission_root)
            )
            checkpoint_path = Path(bindings[runs[0].run_id][0].resolved.path)
            checkpoint_path.write_text("tampered checkpoint", encoding="utf-8")
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)

            with self.assertRaisesRegex(Exception, "digest mismatch"):
                prepare_stage_submission(eval_batch)

            eval_run_dir = (
                Path(eval_batch.submission_root)
                / eval_batch.stage_instances[0].run_dir_rel
            )
            report = json.loads((eval_run_dir / "input_verification.json").read_text())
            self.assertEqual(report["state"], "failed")
            self.assertEqual(
                report["records"][0]["failure_class"], "input_contract_error"
            )
            self.assertIn("digest mismatch", report["records"][0]["reason"])
            materialization = json.loads(
                (
                    Path(eval_batch.submission_root) / "materialization_status.json"
                ).read_text()
            )
            self.assertEqual(materialization["state"], "blocked")
