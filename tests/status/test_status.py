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
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
from tests.support.std import Path, patch, tempfile


class StatusTests(StageBatchSystemTestCase):
    def test_status_package_is_canonical_entry_for_commit_api(self) -> None:
        import slurmforge.status as status

        # status is the unique public entry for stage status writes.
        self.assertTrue(hasattr(status, "commit_stage_status"))
        self.assertTrue(hasattr(status, "commit_attempt"))
        self.assertTrue(hasattr(status, "read_stage_status"))
        # status must not re-export low-level io primitives.
        self.assertFalse(hasattr(status, "read_json"))
        self.assertFalse(hasattr(status, "write_json"))
        self.assertFalse(hasattr(status, "utc_now"))
        # the old contracts.status facade has been removed.
        with self.assertRaises(ModuleNotFoundError):
            __import__("slurmforge.contracts.status")

    def test_status_transition_does_not_regress_success_to_queued(self) -> None:
        from slurmforge.status import StageStatusRecord, commit_stage_status, read_stage_status

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
        from slurmforge.orchestration import render_status_lines
        from slurmforge.slurm import FakeSlurmClient
        from slurmforge.status import read_stage_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            prepared = prepare_stage_submission(batch)
            submit_prepared_stage_batch(prepared, client=client)
            submission_state = read_submission_state(Path(batch.submission_root))
            self.assertEqual(submission_state.ledger_state, "submitted")
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "queued")

            with patch("slurmforge.orchestration.status_view.reconcile_root_submissions") as reconcile:
                render_status_lines(root=Path(batch.submission_root))
                reconcile.assert_not_called()
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "queued")

            with patch("slurmforge.orchestration.status_view.reconcile_root_submissions") as reconcile:
                render_status_lines(root=Path(batch.submission_root), reconcile=True, missing_output_grace_seconds=12)
                reconcile.assert_called_once()

    def test_pipeline_status_reconcile_checks_controller_job(self) -> None:
        from slurmforge.orchestration import render_status_lines

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)

            with (
                patch("slurmforge.orchestration.status_view.reconcile_controller_job") as controller_reconcile,
                patch("slurmforge.orchestration.status_view.reconcile_root_submissions") as stage_reconcile,
            ):
                render_status_lines(root=pipeline_root, reconcile=True, missing_output_grace_seconds=7)
                controller_reconcile.assert_called_once_with(pipeline_root)
                stage_reconcile.assert_called_once()
            self.assertTrue((pipeline_root / "run_status.json").exists())
            self.assertTrue((pipeline_root / "train_eval_pipeline_status.json").exists())
