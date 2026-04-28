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
from tests.support.internal_records import materialize_stage_batch_for_test
import io
import json
import tempfile
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class ResubmitCliTests(StageBatchSystemTestCase):
    def test_resubmit_machine_dry_run_stdout_is_pure_json(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit
        from slurmforge.status.machine import commit_stage_status
        from slurmforge.status.models import StageStatusRecord

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
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)
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

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_resubmit(
                    Namespace(
                        root=str(eval_root),
                        stage="eval",
                        query="state=failed",
                        run_id=[],
                        set=[],
                        dry_run="json",
                        emit_only=False,
                        output=None,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["command"], "resubmit")
            self.assertEqual(payload["state"], "valid")
            self.assertFalse(stdout.getvalue().startswith("[RESUBMIT]"))
            self.assertFalse((eval_root / "derived_batches").exists())

    def test_resubmit_empty_machine_dry_run_stdout_is_pure_json(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
            train_root = Path(train_batch.submission_root)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_resubmit(
                    Namespace(
                        root=str(train_root),
                        stage="train",
                        query="state=failed",
                        run_id=[],
                        set=[],
                        dry_run="json",
                        emit_only=False,
                        output=None,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["command"], "resubmit")
            self.assertEqual(payload["schema_version"], SchemaVersion.DRY_RUN_AUDIT)
            self.assertEqual(payload["plan_kind"], "empty_source_selection")
            self.assertEqual(payload["validation"]["selected_runs"], 0)
            self.assertFalse(stdout.getvalue().startswith("[RESUBMIT]"))

    def test_bad_snapshot_cli_error_has_no_traceback(self) -> None:
        from slurmforge.launcher import main

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
            train_root = Path(train_batch.submission_root)
            (train_root / "spec_snapshot.yaml").unlink()

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(
                    [
                        "resubmit",
                        "--from",
                        str(train_root),
                        "--stage",
                        "train",
                        "--query",
                        "all",
                        "--dry-run=json",
                    ]
                )

            self.assertEqual(code, 2)
            self.assertIn("[ERROR] spec_snapshot.yaml not found", stderr.getvalue())
            self.assertNotIn("Traceback", stdout.getvalue() + stderr.getvalue())
