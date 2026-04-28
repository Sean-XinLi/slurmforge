from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class TestSupportShapeTests(StageBatchSystemTestCase):
    def test_orchestration_returns_data_instead_of_printing(self) -> None:
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/orchestration").rglob("*.py")):
            if "print(" in path.read_text(encoding="utf-8"):
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_orchestration_facade_stays_empty(self) -> None:
        init_text = Path("src/slurmforge/orchestration/__init__.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("from .", init_text)
        self.assertIn("__all__", init_text)

    def test_orchestration_build_is_split_by_workflow(self) -> None:
        self.assertFalse(Path("src/slurmforge/orchestration/build.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/audit.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/estimate.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/pipeline_build.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/stage_build.py").exists())

    def test_orchestration_status_view_is_named_for_status_workflow(self) -> None:
        self.assertFalse(Path("src/slurmforge/orchestration/render.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/status_view.py").exists())

    def test_controller_stage_selection_is_named_for_controller_workflow(self) -> None:
        controller_root = Path("src/slurmforge/controller")
        self.assertFalse((controller_root / "materialization.py").exists())
        self.assertTrue((controller_root / "stage_selection.py").exists())

    def test_starter_template_shared_builders_are_split_by_concern(self) -> None:
        template_root = Path("src/slurmforge/starter/templates")
        self.assertFalse((template_root / "fragments.py").exists())
        for name in ("base.py", "resources.py", "stages.py", "readme.py", "scripts.py"):
            self.assertTrue((template_root / name).exists())

    def test_large_workflow_tests_are_split_by_topic(self) -> None:
        self.assertFalse(Path("tests/e2e/test_pipeline_flow.py").exists())
        self.assertFalse(Path("tests/e2e/test_pipeline_execution.py").exists())
        self.assertFalse(Path("tests/e2e/test_pipeline_records.py").exists())
        self.assertFalse(Path("tests/emit/test_stage_sbatch.py").exists())
        self.assertFalse(Path("tests/resubmit/test_resubmit.py").exists())
        self.assertFalse(Path("tests/starter/test_starter.py").exists())
        self.assertFalse(Path("tests/status/test_reconcile.py").exists())
        self.assertFalse(Path("tests/submission/test_submission.py").exists())
        for path in (
            "tests/e2e/test_file_outputs_flow.py",
            "tests/e2e/test_metric_outputs_flow.py",
            "tests/e2e/test_pipeline_controller_flow.py",
            "tests/e2e/test_pipeline_dry_run.py",
            "tests/e2e/test_plan_record_contracts.py",
            "tests/e2e/test_stage_execution_flow.py",
            "tests/emit/test_stage_sbatch_environment.py",
            "tests/emit/test_stage_sbatch_notifications.py",
            "tests/emit/test_stage_sbatch_torchrun.py",
            "tests/resubmit/test_resubmit_selection.py",
            "tests/resubmit/test_resubmit_materialization.py",
            "tests/resubmit/test_resubmit_status.py",
            "tests/starter/test_generation.py",
            "tests/starter/test_templates.py",
            "tests/starter/test_overwrite.py",
            "tests/starter/test_cli_contract.py",
            "tests/status/test_reconcile_array_jobs.py",
            "tests/status/test_reconcile_attempts.py",
            "tests/status/test_reconcile_missing_outputs.py",
            "tests/status/test_reconcile_squeue.py",
            "tests/submission/test_budget_waves.py",
            "tests/submission/test_submit_failures.py",
            "tests/submission/test_submit_finalizer.py",
            "tests/submission/test_submit_generation.py",
            "tests/submission/test_submit_reconcile.py",
        ):
            self.assertTrue(Path(path).exists())

    def test_test_support_is_split_between_public_workflows_and_internal_records(
        self,
    ) -> None:
        self.assertFalse(Path("tests/support/sforge.py").exists())
        self.assertFalse(Path("tests/support/std.py").exists())
        self.assertTrue(Path("tests/support/public.py").exists())
        self.assertTrue(Path("tests/support/internal_records.py").exists())

    def test_demo_project_helpers_are_split_by_responsibility(self) -> None:
        self.assertTrue(Path("tests/helpers/configs.py").exists())
        self.assertTrue(Path("tests/helpers/profiles.py").exists())
        self.assertTrue(Path("tests/helpers/overlays.py").exists())
        configs_text = Path("tests/helpers/configs.py").read_text(encoding="utf-8")
        self.assertNotIn("def _deep_merge", configs_text)
        self.assertNotIn("def _stage_batch_default_overlay", configs_text)
