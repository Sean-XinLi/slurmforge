from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    render_controller_sbatch,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
)
import json
import tempfile
from pathlib import Path


class PipelineControllerFlowTests(StageBatchSystemTestCase):
    def test_pipeline_controller_is_orchestration_only_and_uses_controller_resources(
        self,
    ) -> None:
        from slurmforge.orchestration.launch import emit_train_eval_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)

            pipeline_root = Path(plan.root_dir)
            manifest = json.loads((pipeline_root / "manifest.json").read_text())
            self.assertEqual(manifest["kind"], "train_eval_pipeline")
            self.assertEqual(manifest["pipeline_kind"], "train_eval_pipeline")
            self.assertTrue((pipeline_root / "train_eval_pipeline_plan.json").exists())
            self.assertTrue(
                (pipeline_root / "stage_batches" / "train" / "batch_plan.json").exists()
            )
            self.assertTrue(
                (pipeline_root / "stage_batches" / "eval" / "batch_plan.json").exists()
            )
            controller_state = json.loads(
                (pipeline_root / "controller" / "controller_state.json").read_text()
            )
            self.assertNotIn("submitted_batches", controller_state)
            controller_sbatch = render_controller_sbatch(plan)
            self.assertIn("#SBATCH --partition=cpu", controller_sbatch)
            self.assertIn("#SBATCH --mem=2G", controller_sbatch)
            self.assertNotIn("train.py", controller_sbatch)
            self.assertNotIn("eval.py", controller_sbatch)

            emitted_plan = compile_train_eval_pipeline_plan(
                spec, submission_root=root / "emitted_pipeline"
            )
            emit_train_eval_pipeline(spec, emitted_plan, submit=False)
            emitted_root = Path(emitted_plan.root_dir)
            self.assertTrue(
                (
                    emitted_root
                    / "stage_batches"
                    / "train"
                    / "submit"
                    / "submit_manifest.json"
                ).exists()
            )
            self.assertFalse(
                (
                    emitted_root
                    / "stage_batches"
                    / "eval"
                    / "submit"
                    / "submit_manifest.json"
                ).exists()
            )

    def test_partial_train_success_keeps_full_and_selected_eval_plans(self) -> None:
        from slurmforge.controller.train_eval_pipeline import run_controller
        from tests.support.slurm import FakeSlurmClient

        class CompletingFakeSlurm(FakeSlurmClient):
            def submit(self, path, *, dependency=None):
                job_id = super().submit(path, dependency=dependency)
                self.set_job_state(job_id, "COMPLETED")
                return job_id

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.entry.args.lr": [0.001, 0.002]},
                    }
                },
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
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
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
            selected_plan = json.loads(
                (eval_root / "selected_batch_plan.json").read_text()
            )
            blocked = json.loads((eval_root / "blocked_runs.json").read_text())
            self.assertEqual(len(full_plan["selected_runs"]), 2)
            self.assertEqual(len(selected_plan["selected_runs"]), 1)
            self.assertEqual(len(blocked["run_ids"]), 1)
