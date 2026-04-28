from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.std import Path


class ModuleShapeTests(StageBatchSystemTestCase):
    def test_orchestration_returns_data_instead_of_printing(self) -> None:
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/orchestration").rglob("*.py")):
            if "print(" in path.read_text(encoding="utf-8"):
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_orchestration_build_is_split_by_workflow(self) -> None:
        self.assertFalse(Path("src/slurmforge/orchestration/build.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/audit.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/estimate.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/pipeline_build.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/stage_build.py").exists())

    def test_orchestration_status_view_is_named_for_status_workflow(self) -> None:
        self.assertFalse(Path("src/slurmforge/orchestration/render.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/status_view.py").exists())

    def test_output_discovery_is_split_by_output_kind(self) -> None:
        discovery_root = Path("src/slurmforge/outputs/discovery")
        self.assertFalse(Path("src/slurmforge/outputs/discovery.py").exists())
        for name in ("context.py", "models.py", "registry.py", "service.py", "writer.py"):
            self.assertTrue((discovery_root / name).exists())
        for name in ("file.py", "files.py", "manifest.py", "metric.py"):
            self.assertTrue((discovery_root / "handlers" / name).exists())
        service_text = (discovery_root / "service.py").read_text(encoding="utf-8")
        self.assertNotIn("if output_cfg.kind ==", service_text)
        self.assertNotIn("elif output_cfg.kind ==", service_text)

    def test_spec_models_resolver_and_snapshot_boundaries_stay_split(self) -> None:
        self.assertFalse(Path("src/slurmforge/spec/models.py").exists())
        self.assertTrue(Path("src/slurmforge/spec/models/experiment.py").exists())
        self.assertFalse(Path("src/slurmforge/resolver/core.py").exists())
        self.assertFalse(Path("src/slurmforge/resolver/sources.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/binding_builders.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/output_refs.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/prior_source.py").exists())

        violations: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "_load_snapshot_yaml" in text or "load_snapshot_yaml" in text:
                violations.append(str(path))
            if path != Path("src/slurmforge/spec/snapshot.py") and "spec_snapshot.yaml" in text and "yaml.safe_load" in text:
                violations.append(f"{path} reads spec snapshots directly")
            if "resolver.core" in text or "resolver.sources" in text:
                violations.append(str(path))
            if "from .core import" in text and Path("src/slurmforge/resolver") in path.parents:
                violations.append(str(path))
            if "from .sources import" in text and Path("src/slurmforge/resolver") in path.parents:
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_plans_package_is_not_a_wide_public_model_facade(self) -> None:
        plans_init = Path("src/slurmforge/plans/__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("from .", plans_init)
        self.assertIn("__all__", plans_init)

    def test_planner_payloads_are_split_by_payload_kind(self) -> None:
        payload_root = Path("src/slurmforge/planner/payloads")
        self.assertFalse(Path("src/slurmforge/planner/payloads.py").exists())
        self.assertTrue(payload_root.is_dir())
        for name in (
            "__init__.py",
            "bindings.py",
            "entry.py",
            "launcher.py",
            "notifications.py",
            "resources.py",
            "runtime.py",
        ):
            self.assertTrue((payload_root / name).exists())

    def test_spec_stage_parsing_is_split_by_section(self) -> None:
        stage_parse_root = Path("src/slurmforge/spec/stage_parse")
        self.assertFalse(Path("src/slurmforge/spec/parse_stages.py").exists())
        self.assertTrue(Path("src/slurmforge/spec/parse_artifact_store.py").exists())
        self.assertTrue(stage_parse_root.is_dir())
        for name in (
            "__init__.py",
            "before.py",
            "entry.py",
            "gpu_sizing.py",
            "inputs.py",
            "launcher.py",
            "stage.py",
        ):
            self.assertTrue((stage_parse_root / name).exists())

    def test_stage_sbatch_rendering_is_split_by_render_surface(self) -> None:
        stage_render_root = Path("src/slurmforge/emit/stage_render")
        self.assertFalse(Path("src/slurmforge/emit/stage_render.py").exists())
        self.assertTrue(stage_render_root.is_dir())
        for name in ("__init__.py", "group.py", "headers.py", "notification.py"):
            self.assertTrue((stage_render_root / name).exists())

    def test_input_verification_records_are_split_by_responsibility(self) -> None:
        verification_root = Path("src/slurmforge/inputs/verification")
        self.assertFalse(Path("src/slurmforge/inputs/verification_records.py").exists())
        self.assertTrue(verification_root.is_dir())
        for name in ("__init__.py", "digests.py", "path_checks.py", "records.py"):
            self.assertTrue((verification_root / name).exists())

    def test_executor_attempt_transaction_is_split(self) -> None:
        self.assertTrue(Path("src/slurmforge/executor/attempt.py").exists())
        self.assertTrue(Path("src/slurmforge/executor/runner.py").exists())
        self.assertTrue(Path("src/slurmforge/executor/finalize.py").exists())
        stage_text = Path("src/slurmforge/executor/stage.py").read_text(encoding="utf-8")
        self.assertNotIn("subprocess.run", stage_text)
        self.assertNotIn("discover_stage_outputs", stage_text)
        self.assertNotIn("require_runtime_contract", stage_text)

    def test_resolver_explicit_sources_are_split(self) -> None:
        self.assertFalse(Path("src/slurmforge/resolver/explicit.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/explicit").is_dir())
        for name in ("external_path.py", "stage_batch.py", "run.py"):
            self.assertTrue(Path("src/slurmforge/resolver/explicit", name).exists())

    def test_starter_template_shared_builders_are_split_by_concern(self) -> None:
        template_root = Path("src/slurmforge/starter/templates")
        self.assertFalse((template_root / "fragments.py").exists())
        for name in ("base.py", "resources.py", "stages.py", "readme.py", "scripts.py"):
            self.assertTrue((template_root / name).exists())

    def test_large_workflow_tests_are_split_by_topic(self) -> None:
        self.assertFalse(Path("tests/e2e/test_pipeline_flow.py").exists())
        self.assertFalse(Path("tests/resubmit/test_resubmit.py").exists())
        self.assertFalse(Path("tests/starter/test_starter.py").exists())
        for path in (
            "tests/e2e/test_pipeline_execution.py",
            "tests/e2e/test_pipeline_records.py",
            "tests/resubmit/test_resubmit_selection.py",
            "tests/resubmit/test_resubmit_materialization.py",
            "tests/resubmit/test_resubmit_status.py",
            "tests/starter/test_generation.py",
            "tests/starter/test_templates.py",
            "tests/starter/test_overwrite.py",
            "tests/starter/test_cli_contract.py",
        ):
            self.assertTrue(Path(path).exists())

    def test_test_support_is_split_between_public_workflows_and_internal_records(self) -> None:
        self.assertFalse(Path("tests/support/sforge.py").exists())
        self.assertTrue(Path("tests/support/public.py").exists())
        self.assertTrue(Path("tests/support/internal_records.py").exists())

    def test_root_path_inference_has_single_source(self) -> None:
        self.assertTrue(Path("src/slurmforge/root_paths.py").exists())
        self.assertFalse(Path("src/slurmforge/root_model/paths.py").exists())
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "_pipeline_root_for_batch_root" in text or "_parent_pipeline_root" in text:
                violations.append(str(path))
            if "from .paths import parent_pipeline_root_for_stage_batch" in text:
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_cli_stage_common_is_split_by_concern(self) -> None:
        self.assertFalse(Path("src/slurmforge/cli/stage_common.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/args.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/builders.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/dry_run.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/render.py").exists())

    def test_notification_finalizer_runtime_is_not_submission_runtime(self) -> None:
        submission_finalizer = Path("src/slurmforge/submission/finalizer.py").read_text(encoding="utf-8")
        self.assertNotIn("deliver_notification", submission_finalizer)
        self.assertNotIn("load_notification_summary_input", submission_finalizer)
        self.assertNotIn("def run_finalizer", submission_finalizer)
        runtime = Path("src/slurmforge/notifications/finalizer_runtime.py").read_text(encoding="utf-8")
        self.assertIn("def run_finalizer", runtime)
        self.assertIn("deliver_notification", runtime)

    def test_storage_layout_is_split_by_root_type(self) -> None:
        self.assertFalse(Path("src/slurmforge/storage/layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/batch_layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/train_eval_pipeline_layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/status_seed.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/controller_seed.py").exists())

    def test_sizing_package_stays_plan_agnostic(self) -> None:
        self.assertFalse(Path("src/slurmforge/sizing/estimate.py").exists())
        sizing_text = "\n".join(path.read_text(encoding="utf-8") for path in Path("src/slurmforge/sizing").rglob("*.py"))
        self.assertNotIn("hasattr(plan", sizing_text)
        self.assertNotIn("StageBatchPlan", sizing_text)
        self.assertNotIn("TrainEvalPipelinePlan", sizing_text)

    def test_spec_models_do_not_import_parser(self) -> None:
        text = "\n".join(path.read_text(encoding="utf-8") for path in Path("src/slurmforge/spec/models").rglob("*.py"))
        self.assertNotIn("parse_experiment_spec", text)
        self.assertNotIn("from .parser import", text)

    def test_cli_flags_are_kebab_case_only(self) -> None:
        old_flags = ("--dry_run", "--emit_only", "--project_root")
        checked = list(Path("src/slurmforge").rglob("*.py")) + [
            Path("README.md"),
            Path("docs/record-contract.md"),
        ]
        violations: list[str] = []
        for path in checked:
            text = path.read_text(encoding="utf-8")
            for flag in old_flags:
                if flag in text:
                    violations.append(f"{path} contains {flag}")
        self.assertEqual(violations, [])

    def test_run_record_contract_is_split_into_topic_docs(self) -> None:
        record_index = Path("docs/record-contract.md").read_text(encoding="utf-8")
        self.assertLess(len(record_index.splitlines()), 20)
        for name in ("artifacts.md", "planning.md", "runtime.md", "status.md", "submission.md"):
            self.assertTrue(Path("docs/records", name).exists())

    def test_import_linter_config_was_removed(self) -> None:
        self.assertFalse(Path(".importlinter").exists())
        self.assertNotIn("lint-imports", Path("README.md").read_text(encoding="utf-8"))
