from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    SchemaVersion,
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
from tests.support.std import Namespace, Path, json, tempfile

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
