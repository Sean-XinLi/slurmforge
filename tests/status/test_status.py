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
import json
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
                "slurmforge.orchestration.status_read_model.reconcile_root_submissions"
            ) as reconcile:
                render_status_lines(root=Path(batch.submission_root))
                reconcile.assert_not_called()
            status = read_stage_status(run_dir)
            assert status is not None
            self.assertEqual(status.state, "queued")

            with patch(
                "slurmforge.orchestration.status_read_model.reconcile_root_submissions"
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
                "slurmforge.orchestration.status_read_model.reconcile_root_submissions"
            ) as stage_reconcile:
                render_status_lines(
                    root=pipeline_root, reconcile=True, missing_output_grace_seconds=7
                )
                stage_reconcile.assert_called_once()
            self.assertTrue((pipeline_root / "run_status.json").exists())
            self.assertTrue(
                (pipeline_root / "train_eval_pipeline_status.json").exists()
            )

    def test_pipeline_status_renders_all_control_records(self) -> None:
        from slurmforge.orchestration.status_view import render_status_lines
        from slurmforge.storage.workflow import (
            read_workflow_status,
            write_workflow_status,
        )
        from slurmforge.storage.workflow_status_records import (
            WorkflowStatusControlJobRecord,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            workflow_status = read_workflow_status(pipeline_root)
            assert workflow_status is not None
            workflow_status.state = "failed"
            workflow_status.reason = "control action failed"
            workflow_status.control_jobs = {
                "terminal_notification:train_eval_pipeline_finished": WorkflowStatusControlJobRecord(
                    key="terminal_notification:train_eval_pipeline_finished",
                    kind="terminal_notification",
                    target_kind="workflow",
                    target_id="train_eval_pipeline_finished",
                    state="submitted",
                    sbatch_paths=("notify.sbatch",),
                    scheduler_job_ids=("1001", "1002"),
                ),
                "dispatch_catchup_gate:train_initial": WorkflowStatusControlJobRecord(
                    key="dispatch_catchup_gate:train_initial",
                    kind="dispatch_catchup_gate",
                    target_kind="dispatch",
                    target_id="train_initial",
                    state="failed",
                    sbatch_paths=("gate.sbatch",),
                    reason="sbatch rejected",
                ),
            }

            write_workflow_status(pipeline_root, workflow_status)

            output = "\n".join(render_status_lines(root=pipeline_root))

            self.assertIn(
                "terminal_notification:train_eval_pipeline_finished=1001,1002",
                output,
            )
            self.assertIn(
                "dispatch_catchup_gate:train_initial=failed(sbatch rejected)",
                output,
            )

    def test_malformed_workflow_status_is_rejected(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.storage.workflow import read_workflow_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            status_path = pipeline_root / "control" / "workflow_status.json"
            status_path.write_text(
                json.dumps({"schema_version": 1, "state": "planned"}),
                encoding="utf-8",
            )

            with self.assertRaises(RecordContractError):
                read_workflow_status(pipeline_root)
