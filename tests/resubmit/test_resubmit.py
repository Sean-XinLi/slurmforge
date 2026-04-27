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
from tests.support.std import Namespace, Path, io, json, patch, redirect_stdout, tempfile


class ResubmitTests(StageBatchSystemTestCase):
    def test_resubmit_rebinds_upstream_output_from_pipeline_root(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)

            handle_resubmit(
                Namespace(
                    root=plan.root_dir,
                    stage="eval",
                    query="state=planned",
                    run_id=[],
                    set=[],
                    dry_run=False,
                    emit_only=True,
                )
            )

            resubmit_roots = sorted((Path(plan.root_dir) / "derived_batches").glob("eval_batch_*"))
            self.assertEqual(len(resubmit_roots), 1)
            binding_files = list(resubmit_roots[0].glob("runs/*/input_bindings.json"))
            self.assertEqual(len(binding_files), 1)
            payload = json.loads(binding_files[0].read_text())
            checkpoint = payload["bindings"]["checkpoint"]
            self.assertEqual(checkpoint["source"]["kind"], "upstream_output")
            self.assertTrue(checkpoint["resolved"]["path"].endswith(".pt"))
            self.assertTrue((resubmit_roots[0] / "source_plan.json").exists())
            self.assertTrue((resubmit_roots[0] / "source_lineage.json").exists())

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

    def test_resubmit_repeated_emits_create_distinct_batch_roots(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)

            for _ in range(2):
                handle_resubmit(
                    Namespace(
                        root=plan.root_dir,
                        stage="eval",
                        query="state=planned",
                        run_id=[],
                        set=[],
                        dry_run=False,
                        emit_only=True,
                    )
                )

            resubmit_roots = sorted((Path(plan.root_dir) / "derived_batches").glob("eval_batch_*"))
            self.assertEqual(len(resubmit_roots), 2)
            self.assertNotEqual(resubmit_roots[0].name, resubmit_roots[1].name)

    def test_resubmit_root_reservation_is_new_only(self) -> None:
        from slurmforge.storage.materialization import reserve_derived_stage_batch_root

        with tempfile.TemporaryDirectory() as tmp:
            source_root = Path(tmp)

            first = reserve_derived_stage_batch_root(source_root, "eval_batch_contract")
            second = reserve_derived_stage_batch_root(source_root, "eval_batch_contract")

            self.assertEqual(first.batch_id, "eval_batch_contract")
            self.assertEqual(second.batch_id, "eval_batch_contract_r0002")
            self.assertTrue(first.root.exists())
            self.assertTrue(second.root.exists())
            self.assertNotEqual(first.root, second.root)

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

    def test_eval_batch_root_resubmit_uses_lineage_index(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit
        from slurmforge.status import StageStatusRecord, commit_stage_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(train_batch.submission_root), 1, 0), 0)
            runs, bindings = upstream_bindings_from_train_batch(spec, Path(train_batch.submission_root))
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)
            eval_root = Path(eval_batch.submission_root)
            lineage = json.loads((eval_root / "lineage_index.json").read_text())
            self.assertIn(str(Path(train_batch.submission_root).resolve()), lineage["source_roots"])

            eval_instance = eval_batch.stage_instances[0]
            eval_run_dir = eval_root / eval_instance.run_dir_rel
            commit_stage_status(
                eval_run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=eval_instance.stage_instance_id,
                    run_id=eval_instance.run_id,
                    stage_name=eval_instance.stage_name,
                    state="failed",
                    reason="intentional test failure",
                ),
            )

            handle_resubmit(
                Namespace(
                    root=str(eval_root),
                    stage="eval",
                    query="state=failed",
                    run_id=[],
                    set=[],
                    dry_run=False,
                    emit_only=True,
                )
            )

            resubmit_roots = sorted((eval_root / "derived_batches").glob("eval_batch_*"))
            self.assertEqual(len(resubmit_roots), 1)
            payload = json.loads(next(resubmit_roots[0].glob("runs/*/input_bindings.json")).read_text())
            checkpoint = payload["bindings"]["checkpoint"]
            self.assertEqual(checkpoint["source"]["kind"], "upstream_output")
            self.assertTrue(checkpoint["resolved"]["path"].endswith(".pt"))
            self.assertEqual(
                checkpoint["resolution"]["producer_root"],
                str(Path(train_batch.submission_root).resolve()),
            )

    def test_resubmit_blocks_emit_when_lineage_checkpoint_is_missing(self) -> None:
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
            self.assertTrue(checkpoint_path.exists())
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
            eval_run_dir = eval_root / eval_instance.run_dir_rel
            commit_stage_status(
                eval_run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=eval_instance.stage_instance_id,
                    run_id=eval_instance.run_id,
                    stage_name=eval_instance.stage_name,
                    state="failed",
                    reason="intentional test failure",
                ),
            )
            checkpoint_path.unlink()

            with self.assertRaisesRegex(Exception, "input path does not exist"):
                handle_resubmit(
                    Namespace(
                        root=str(eval_root),
                        stage="eval",
                        query="state=failed",
                        run_id=[],
                        set=[],
                        dry_run=False,
                        emit_only=True,
                    )
                )

            resubmit_roots = sorted((eval_root / "derived_batches").glob("eval_batch_*"))
            self.assertEqual(len(resubmit_roots), 1)
            self.assertTrue((resubmit_roots[0] / "source_plan.json").exists())
            self.assertTrue((resubmit_roots[0] / "source_lineage.json").exists())
            self.assertFalse((resubmit_roots[0] / "submit" / "submit_manifest.json").exists())
            report = json.loads(next(resubmit_roots[0].glob("runs/*/input_verification.json")).read_text())
            self.assertEqual(report["state"], "failed")
            self.assertEqual(report["records"][0]["failure_class"], "input_contract_error")
            materialization = json.loads((resubmit_roots[0] / "materialization_status.json").read_text())
            self.assertEqual(materialization["state"], "blocked")
            self.assertEqual(materialization["failure_class"], "input_contract_error")
            status = json.loads(next(resubmit_roots[0].glob("runs/*/status.json")).read_text())
            self.assertEqual(status["state"], "blocked")
            self.assertEqual(status["failure_class"], "input_contract_error")

            from slurmforge.cli.status import render_status

            rendered = io.StringIO()
            with redirect_stdout(rendered):
                render_status(root=resubmit_roots[0])
            output = rendered.getvalue()
            self.assertIn("materialization stage=eval state=blocked", output)
            self.assertIn("state=blocked", output)

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

    def test_resubmit_replans_inputs_after_overrides(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            replacement = root / "replacement.pt"
            replacement.write_text("replacement", encoding="utf-8")

            handle_resubmit(
                Namespace(
                    root=plan.root_dir,
                    stage="eval",
                    query="state=planned",
                    run_id=[],
                    set=[
                        "stages.eval.inputs.checkpoint.source.kind=external_path",
                        f"stages.eval.inputs.checkpoint.source.path={replacement}",
                    ],
                    dry_run=False,
                    emit_only=True,
                )
            )

            resubmit_roots = sorted((Path(plan.root_dir) / "derived_batches").glob("eval_batch_*"))
            self.assertEqual(len(resubmit_roots), 1)
            payload = json.loads(next(resubmit_roots[0].glob("runs/*/input_bindings.json")).read_text())
            checkpoint = payload["bindings"]["checkpoint"]
            self.assertEqual(checkpoint["source"]["kind"], "external_path")
            self.assertEqual(checkpoint["source"]["path"], str(replacement))
            self.assertEqual(checkpoint["resolved"]["path"], str(replacement.resolve()))
