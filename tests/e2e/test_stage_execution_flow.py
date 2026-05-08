from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
    write_stage_submit_files,
)
from tests.support.internal_records import (
    load_stage_outputs,
    materialize_stage_batch_for_test,
)
import json
import tempfile
from pathlib import Path


class StageExecutionFlowTests(StageBatchSystemTestCase):
    def test_train_and_eval_are_separate_attempts_with_file_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))

            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
            train_paths = write_stage_submit_files(train_batch)
            self.assertNotIn("eval.py", train_paths[0].read_text())

            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            outputs = load_stage_outputs(train_run_dir)
            assert outputs is not None
            self.assertIn("checkpoint", outputs.outputs)
            checkpoint_ref = outputs.outputs["checkpoint"]
            self.assertEqual(checkpoint_ref.schema_version, 1)
            self.assertEqual(checkpoint_ref.output_name, "checkpoint")
            self.assertTrue(checkpoint_ref.managed)
            self.assertEqual(checkpoint_ref.producer_attempt_id, "0001")
            self.assertTrue(checkpoint_ref.digest)
            self.assertTrue(Path(checkpoint_ref.path).exists())
            self.assertTrue(
                (
                    train_run_dir
                    / "attempts"
                    / "0001"
                    / "artifacts"
                    / "artifact_manifest.json"
                ).exists()
            )
            self.assertTrue(
                (
                    train_run_dir
                    / "attempts"
                    / "0001"
                    / "outputs"
                    / "stage_outputs.json"
                ).exists()
            )
            root_ref = json.loads((train_run_dir / "root_ref.json").read_text())
            self.assertEqual(
                root_ref["stage_batch_root"],
                str(Path(train_batch.submission_root).resolve()),
            )
            run_status = json.loads(
                (Path(train_batch.submission_root) / "run_status.json").read_text()
            )
            self.assertEqual(run_status["runs"][0]["state"], "success")

            attempt = json.loads(
                (train_run_dir / "attempts" / "0001" / "attempt.json").read_text()
            )
            self.assertEqual(attempt["exit_code"], 0)
            self.assertEqual(attempt["attempt_source"], "executor")
            self.assertEqual(attempt["attempt_state"], "final")
            self.assertTrue(attempt["started_by_executor"])
            self.assertTrue(attempt["executor_started_at"])
            self.assertTrue(attempt["executor_finished_at"])
            manifest = json.loads(
                (
                    train_run_dir
                    / "attempts"
                    / "0001"
                    / "artifacts"
                    / "artifact_manifest.json"
                ).read_text()
            )
            self.assertEqual(manifest["artifacts"][0]["strategy"], "copy")

            runs, bindings = upstream_bindings_from_train_batch(
                spec, Path(train_batch.submission_root)
            )
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref="test",
            )
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)
            eval_paths = write_stage_submit_files(eval_batch)
            self.assertNotIn("train.py", eval_paths[0].read_text())

            eval_run_dir = (
                Path(eval_batch.submission_root)
                / eval_batch.stage_instances[0].run_dir_rel
            )
            binding_payload = json.loads(
                (eval_run_dir / "input_bindings.json").read_text()
            )
            self.assertEqual(
                binding_payload["bindings"]["checkpoint"]["source"]["kind"],
                "upstream_output",
            )
            self.assertEqual(
                binding_payload["bindings"]["checkpoint"]["resolved"]["kind"], "path"
            )
            self.assertTrue(
                binding_payload["bindings"]["checkpoint"]["resolved"]["path"].endswith(
                    ".pt"
                )
            )
            self.assertEqual(
                execute_stage_task(Path(eval_batch.submission_root), 1, 0), 0
            )
