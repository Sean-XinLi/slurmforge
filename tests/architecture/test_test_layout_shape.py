from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class TestLayoutShapeTests(StageBatchSystemTestCase):
    def test_large_workflow_tests_are_split_by_topic(self) -> None:
        topic_files = {
            "tests/e2e": (
                "test_file_outputs_flow.py",
                "test_metric_artifact_outputs_flow.py",
                "test_metric_value_inputs_flow.py",
                "test_pipeline_dry_run.py",
                "test_plan_record_contracts.py",
                "test_stage_execution_flow.py",
            ),
            "tests/control": (
                "pipeline_helpers.py",
                "pipeline_overlays.py",
                "pipeline_scenarios.py",
                "test_control_submission_ledger.py",
                "test_pipeline_budget.py",
                "test_pipeline_control.py",
                "test_pipeline_initial_submit.py",
                "test_pipeline_release_policy.py",
                "test_terminal_notification.py",
                "test_workflow_state_contract.py",
            ),
            "tests/executor": (
                "test_before_steps.py",
                "test_diagnostics.py",
                "test_input_preflight.py",
                "test_runtime_contract.py",
            ),
            "tests/planner": (
                "test_dry_run_audit.py",
                "test_pipeline_plan.py",
                "test_stage_batch_resources.py",
                "test_stage_batch_runs.py",
            ),
            "tests/resubmit": (
                "test_resubmit_materialization.py",
                "test_resubmit_roots.py",
                "test_resubmit_status.py",
                "test_resubmit_upstream_inputs.py",
            ),
            "tests/starter": (
                "test_cli_contract.py",
                "test_generated_readme.py",
                "test_generated_yaml.py",
                "test_generation.py",
                "test_overwrite.py",
                "test_templates.py",
            ),
            "tests/status": (
                "test_reconcile_array_jobs.py",
                "test_reconcile_attempts.py",
                "test_reconcile_missing_outputs.py",
                "test_reconcile_squeue.py",
            ),
            "tests/submission": (
                "test_budget_waves.py",
                "test_submit_failures.py",
                "test_submit_finalizer.py",
                "test_submit_generation.py",
                "test_submit_reconcile.py",
            ),
        }
        for directory, filenames in topic_files.items():
            for filename in filenames:
                self.assertTrue(Path(directory, filename).exists())
