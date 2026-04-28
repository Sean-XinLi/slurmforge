from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    load_stage_outputs,
    render_controller_sbatch,
    upstream_bindings_from_train_batch,
    write_demo_project,
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
    write_stage_submit_files,
)
from tests.support.std import Namespace, Path, json, tempfile

class TrainEvalPipelineFlowTests(StageBatchSystemTestCase):
    def test_train_and_eval_are_separate_attempts_with_file_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))

            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            train_paths = write_stage_submit_files(train_batch)
            self.assertNotIn("eval.py", train_paths[0].read_text())

            self.assertEqual(execute_stage_task(Path(train_batch.submission_root), 1, 0), 0)
            train_run_dir = Path(train_batch.submission_root) / train_batch.stage_instances[0].run_dir_rel
            outputs = load_stage_outputs(train_run_dir)
            assert outputs is not None
            self.assertIn("checkpoint", outputs["outputs"])
            checkpoint_ref = outputs["outputs"]["checkpoint"]
            self.assertEqual(checkpoint_ref["schema_version"], 1)
            self.assertEqual(checkpoint_ref["output_name"], "checkpoint")
            self.assertTrue(checkpoint_ref["managed"])
            self.assertEqual(checkpoint_ref["producer_attempt_id"], "0001")
            self.assertTrue(checkpoint_ref["digest"])
            self.assertTrue(Path(checkpoint_ref["path"]).exists())
            self.assertTrue((train_run_dir / "attempts" / "0001" / "artifacts" / "artifact_manifest.json").exists())
            self.assertTrue((train_run_dir / "attempts" / "0001" / "outputs" / "stage_outputs.json").exists())
            root_ref = json.loads((train_run_dir / "root_ref.json").read_text())
            self.assertEqual(root_ref["stage_batch_root"], str(Path(train_batch.submission_root).resolve()))
            run_status = json.loads((Path(train_batch.submission_root) / "run_status.json").read_text())
            self.assertEqual(run_status["runs"][0]["state"], "success")

            attempt = json.loads((train_run_dir / "attempts" / "0001" / "attempt.json").read_text())
            self.assertEqual(attempt["exit_code"], 0)
            self.assertEqual(attempt["attempt_source"], "executor")
            self.assertEqual(attempt["attempt_state"], "final")
            self.assertTrue(attempt["started_by_executor"])
            self.assertTrue(attempt["executor_started_at"])
            self.assertTrue(attempt["executor_finished_at"])
            manifest = json.loads(
                (train_run_dir / "attempts" / "0001" / "artifacts" / "artifact_manifest.json").read_text()
            )
            self.assertEqual(manifest["artifacts"][0]["strategy"], "copy")

            runs, bindings = upstream_bindings_from_train_batch(spec, Path(train_batch.submission_root))
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref="test",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)
            eval_paths = write_stage_submit_files(eval_batch)
            self.assertNotIn("train.py", eval_paths[0].read_text())

            eval_run_dir = Path(eval_batch.submission_root) / eval_batch.stage_instances[0].run_dir_rel
            binding_payload = json.loads((eval_run_dir / "input_bindings.json").read_text())
            self.assertEqual(binding_payload["bindings"]["checkpoint"]["source"]["kind"], "upstream_output")
            self.assertEqual(binding_payload["bindings"]["checkpoint"]["resolved"]["kind"], "path")
            self.assertTrue(binding_payload["bindings"]["checkpoint"]["resolved"]["path"].endswith(".pt"))
            self.assertEqual(execute_stage_task(Path(eval_batch.submission_root), 1, 0), 0)
    def test_pipeline_controller_is_orchestration_only_and_uses_controller_resources(self) -> None:
        from slurmforge.orchestration import emit_train_eval_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)

            pipeline_root = Path(plan.root_dir)
            manifest = json.loads((pipeline_root / "manifest.json").read_text())
            self.assertEqual(manifest["kind"], "train_eval_pipeline")
            self.assertEqual(manifest["pipeline_kind"], "train_eval_pipeline")
            self.assertTrue((pipeline_root / "train_eval_pipeline_plan.json").exists())
            self.assertTrue((pipeline_root / "stage_batches" / "train" / "batch_plan.json").exists())
            self.assertTrue((pipeline_root / "stage_batches" / "eval" / "batch_plan.json").exists())
            controller_state = json.loads((pipeline_root / "controller" / "controller_state.json").read_text())
            self.assertNotIn("submitted_batches", controller_state)
            controller_sbatch = render_controller_sbatch(plan)
            self.assertIn("#SBATCH --partition=cpu", controller_sbatch)
            self.assertIn("#SBATCH --mem=2G", controller_sbatch)
            self.assertNotIn("train.py", controller_sbatch)
            self.assertNotIn("eval.py", controller_sbatch)

            emitted_plan = compile_train_eval_pipeline_plan(spec, submission_root=root / "emitted_pipeline")
            emit_train_eval_pipeline(spec, emitted_plan, submit=False)
            emitted_root = Path(emitted_plan.root_dir)
            self.assertTrue((emitted_root / "stage_batches" / "train" / "submit" / "submit_manifest.json").exists())
            self.assertFalse((emitted_root / "stage_batches" / "eval" / "submit" / "submit_manifest.json").exists())
    def test_machine_dry_run_full_emits_auditable_json_without_materializing(self) -> None:
        from slurmforge.cli.train import handle_train

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            audit_path = root / "audit.json"
            handle_train(
                Namespace(
                    config=str(cfg_path),
                    set=[],
                    project_root=None,
                    dry_run="full",
                    emit_only=False,
                    output=str(audit_path),
                )
            )
            payload = json.loads(audit_path.read_text())
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["command"], "train")
            self.assertEqual(payload["state"], "valid")
            self.assertEqual(payload["validation"]["runtime_contracts"][0]["state"], "verified")
            probe_roles = {
                item["runtime_role"]: item
                for item in payload["validation"]["runtime_contracts"][0]["probes"]
            }
            self.assertEqual(probe_roles["executor"]["state"], "verified")
            self.assertEqual(probe_roles["user"]["state"], "verified")
            self.assertFalse(any((root / "runs").glob("**/batch_plan.json")))
    def test_partial_train_success_keeps_full_and_selected_eval_plans(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from slurmforge.slurm import FakeSlurmClient

        class CompletingFakeSlurm(FakeSlurmClient):
            def submit(self, path, *, dependency=None):
                job_id = super().submit(path, dependency=dependency)
                self.set_job_state(job_id, "COMPLETED")
                return job_id

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={"runs": {"type": "grid", "axes": {"train.entry.args.lr": [0.001, 0.002]}}},
            )
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import argparse",
                        "p = argparse.ArgumentParser()",
                        "p.add_argument('--lr')",
                        "args = p.parse_args()",
                        "if args.lr == '0.002': raise SystemExit(1)",
                        "out = Path('checkpoints')",
                        "out.mkdir(exist_ok=True)",
                        "(out / f'step_{str(args.lr).replace(\".\", \"\")}.pt').write_text('ckpt')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            self.assertNotEqual(execute_stage_task(train_root, 1, 1), 0)

            exit_code = run_controller(
                Path(plan.root_dir),
                client=CompletingFakeSlurm(),
                poll_seconds=0,
                missing_output_grace_seconds=0,
            )
            self.assertEqual(exit_code, 1)
            eval_root = Path(plan.stage_batches["eval"].submission_root)
            full_plan = json.loads((eval_root / "batch_plan.json").read_text())
            selected_plan = json.loads((eval_root / "selected_batch_plan.json").read_text())
            blocked = json.loads((eval_root / "blocked_runs.json").read_text())
            self.assertEqual(len(full_plan["selected_runs"]), 2)
            self.assertEqual(len(selected_plan["selected_runs"]), 1)
            self.assertEqual(len(blocked["run_ids"]), 1)
