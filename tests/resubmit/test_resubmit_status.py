from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    SchemaVersion,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
)
from tests.support.internal_records import write_stage_batch_layout
import io
import json
import tempfile
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path


class ResubmitTests(StageBatchSystemTestCase):
    def test_resubmit_blocks_emit_when_lineage_checkpoint_is_missing(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit
        from slurmforge.status.machine import commit_stage_status
        from slurmforge.status.models import StageStatusRecord

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

            resubmit_roots = sorted(
                (eval_root / "derived_batches").glob("eval_batch_*")
            )
            self.assertEqual(len(resubmit_roots), 1)
            self.assertTrue((resubmit_roots[0] / "source_plan.json").exists())
            self.assertTrue((resubmit_roots[0] / "source_lineage.json").exists())
            self.assertFalse(
                (resubmit_roots[0] / "submit" / "submit_manifest.json").exists()
            )
            report = json.loads(
                next(
                    resubmit_roots[0].glob("runs/*/input_verification.json")
                ).read_text()
            )
            self.assertEqual(report["state"], "failed")
            self.assertEqual(
                report["records"][0]["failure_class"], "input_contract_error"
            )
            materialization = json.loads(
                (resubmit_roots[0] / "materialization_status.json").read_text()
            )
            self.assertEqual(materialization["state"], "blocked")
            self.assertEqual(materialization["failure_class"], "input_contract_error")
            status = json.loads(
                next(resubmit_roots[0].glob("runs/*/status.json")).read_text()
            )
            self.assertEqual(status["state"], "blocked")
            self.assertEqual(status["failure_class"], "input_contract_error")

            from slurmforge.orchestration.status_view import render_status_lines

            rendered = io.StringIO()
            with redirect_stdout(rendered):
                for line in render_status_lines(root=resubmit_roots[0]):
                    print(line)
            output = rendered.getvalue()
            self.assertIn("materialization stage=eval state=blocked", output)
            self.assertIn("state=blocked", output)
