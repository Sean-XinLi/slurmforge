from __future__ import annotations

from pathlib import Path
import json
import tempfile

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_stage_batch_for_test
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    prepare_stage_submission,
    upstream_bindings_from_train_batch,
    write_demo_project,
)


class ExecutorInputPreflightTests(StageBatchSystemTestCase):
    def test_executor_preflight_missing_input_fails_before_user_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
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
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)
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
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
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
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)

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
