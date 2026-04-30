from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    SchemaVersion,
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    prepare_stage_submission,
    read_submission_state,
    submit_prepared_stage_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
    materialize_stage_batch_for_test,
)
import tempfile
from pathlib import Path
from unittest.mock import patch


class StatusTests(StageBatchSystemTestCase):
    def test_status_package_facade_stays_empty(self) -> None:
        import slurmforge.status as status

        self.assertEqual(status.__all__, [])
        self.assertFalse(hasattr(status, "commit_stage_status"))
        self.assertFalse(hasattr(status, "commit_attempt"))
        self.assertFalse(hasattr(status, "read_stage_status"))
        self.assertFalse(hasattr(status, "read_json"))
        self.assertFalse(hasattr(status, "write_json"))
        self.assertFalse(hasattr(status, "utc_now"))
        with self.assertRaises(ModuleNotFoundError):
            __import__("slurmforge.contracts.status")

    def test_status_transition_does_not_regress_success_to_queued(self) -> None:
        from slurmforge.status.machine import commit_stage_status
        from slurmforge.status.models import StageStatusRecord
        from slurmforge.status.reader import read_stage_status

        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            commit_stage_status(
                run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id="run_1.train",
                    run_id="run_1",
                    stage_name="train",
                    state="success",
                    latest_attempt_id="0001",
                ),
            )
            commit_stage_status(
                run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id="run_1.train",
                    run_id="run_1",
                    stage_name="train",
                    state="queued",
                    reason="late submit marker",
                ),
            )
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "success")

    def test_status_is_read_only_unless_reconcile_is_requested(self) -> None:
        from slurmforge.orchestration.status_view import render_status_lines
        from tests.support.slurm import FakeSlurmClient
        from slurmforge.status.reader import read_stage_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            prepared = prepare_stage_submission(batch)
            submit_prepared_stage_batch(prepared, client=client)
            submission_state = read_submission_state(Path(batch.submission_root))
            self.assertEqual(submission_state.ledger_state, "submitted")
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "queued")

            with patch(
                "slurmforge.orchestration.status_view.reconcile_root_submissions"
            ) as reconcile:
                render_status_lines(root=Path(batch.submission_root))
                reconcile.assert_not_called()
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "queued")

            with patch(
                "slurmforge.orchestration.status_view.reconcile_root_submissions"
            ) as reconcile:
                render_status_lines(
                    root=Path(batch.submission_root),
                    reconcile=True,
                    missing_output_grace_seconds=12,
                )
                reconcile.assert_called_once()

    def test_pipeline_status_reconcile_checks_execution_batches(self) -> None:
        from slurmforge.orchestration.status_view import render_status_lines

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)

            with patch(
                "slurmforge.orchestration.status_view.reconcile_root_submissions"
            ) as stage_reconcile:
                render_status_lines(
                    root=pipeline_root, reconcile=True, missing_output_grace_seconds=7
                )
                stage_reconcile.assert_called_once()
            self.assertTrue((pipeline_root / "run_status.json").exists())
            self.assertTrue(
                (pipeline_root / "train_eval_pipeline_status.json").exists()
            )
