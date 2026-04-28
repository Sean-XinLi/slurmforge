from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    SchemaVersion,
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    read_submission_ledger,
    upstream_bindings_from_train_batch,
    write_demo_project,
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
from tests.support.std import Namespace, Path, patch, tempfile

class ResubmitTests(StageBatchSystemTestCase):
    def test_resubmit_true_submit_writes_new_batch_ledger(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit
        from slurmforge.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client = FakeSlurmClient()

            with patch("slurmforge.submission.submitter.SlurmClient", return_value=client):
                handle_resubmit(
                    Namespace(
                        root=plan.root_dir,
                        stage="eval",
                        query="state=planned",
                        run_id=[],
                        set=[],
                        dry_run=False,
                        emit_only=False,
                    )
                )

            resubmit_roots = sorted((Path(plan.root_dir) / "derived_batches").glob("eval_batch_*"))
            self.assertEqual(len(resubmit_roots), 1)
            ledger = read_submission_ledger(resubmit_roots[0])
            assert ledger is not None
            self.assertEqual(ledger.state, "submitted")
            self.assertEqual(len(client.submissions), 1)
    def test_resubmit_validates_overrides_before_materialization(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)

            with self.assertRaisesRegex(Exception, "launcher.type"):
                handle_resubmit(
                    Namespace(
                        root=plan.root_dir,
                        stage="eval",
                        query="state=planned",
                        run_id=[],
                        set=["stages.eval.launcher.type=bad_launcher"],
                        dry_run=True,
                        emit_only=False,
                    )
                )

            self.assertFalse((Path(plan.root_dir) / "derived_batches").exists())
    def test_resubmit_dry_run_does_not_verify_missing_lineage_checkpoint(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit
        from slurmforge.status import StageStatusRecord, commit_stage_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(train_batch.submission_root), 1, 0), 0)
            runs, bindings = upstream_bindings_from_train_batch(spec, Path(train_batch.submission_root))
            checkpoint_path = Path(bindings[runs[0].run_id][0].resolved.path)
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)
            eval_root = Path(eval_batch.submission_root)
            eval_instance = eval_batch.stage_instances[0]
            commit_stage_status(
                eval_root / eval_instance.run_dir_rel,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=eval_instance.stage_instance_id,
                    run_id=eval_instance.run_id,
                    stage_name=eval_instance.stage_name,
                    state="failed",
                ),
            )
            checkpoint_path.unlink()

            handle_resubmit(
                Namespace(
                    root=str(eval_root),
                    stage="eval",
                    query="state=failed",
                    run_id=[],
                    set=[],
                    dry_run=True,
                    emit_only=False,
                )
            )

            self.assertFalse((eval_root / "derived_batches").exists())
