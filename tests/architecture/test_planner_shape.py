from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class PlannerShapeTests(StageBatchSystemTestCase):
    def test_plans_package_is_not_a_wide_public_model_facade(self) -> None:
        plans_init = Path("src/slurmforge/plans/__init__.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("from .", plans_init)
        self.assertIn("__all__", plans_init)

    def test_planner_package_is_not_a_wide_workflow_facade(self) -> None:
        planner_init = Path("src/slurmforge/planner/__init__.py").read_text(
            encoding="utf-8"
        )
        self.assertFalse(Path("src/slurmforge/planner/core.py").exists())
        self.assertNotIn("from .", planner_init)
        self.assertIn("__all__", planner_init)

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

    def test_sizing_package_stays_plan_agnostic(self) -> None:
        self.assertFalse(Path("src/slurmforge/sizing/estimate.py").exists())
        sizing_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in Path("src/slurmforge/sizing").rglob("*.py")
        )
        self.assertNotIn("hasattr(plan", sizing_text)
        self.assertNotIn("StageBatchPlan", sizing_text)
        self.assertNotIn("TrainEvalPipelinePlan", sizing_text)

    def test_resource_estimate_is_report_layer_not_planner_or_sizing(self) -> None:
        self.assertFalse(Path("src/slurmforge/planner/resource_estimate.py").exists())
        estimate_root = Path("src/slurmforge/resource_estimates")
        for name in ("__init__.py", "models.py", "build.py", "render.py"):
            self.assertTrue((estimate_root / name).exists())

        sizing_text = Path("src/slurmforge/sizing/models.py").read_text(
            encoding="utf-8"
        )
        estimate_models = (estimate_root / "models.py").read_text(encoding="utf-8")
        estimate_render = (estimate_root / "render.py").read_text(encoding="utf-8")

        self.assertNotIn("ResourceGroupEstimate", sizing_text)
        self.assertNotIn("StageResourceEstimate", sizing_text)
        self.assertNotIn("ExperimentResourceEstimate", sizing_text)
        self.assertNotIn("RESOURCE_ESTIMATE", sizing_text)
        self.assertIn("run_sizing: tuple[GpuSizingResolution, ...]", estimate_models)
        self.assertNotIn("dict[str, Any]", estimate_models)
        self.assertNotIn("sizing.get(", estimate_render)

    def test_stage_instance_lineage_is_a_plan_model(self) -> None:
        plan_text = Path("src/slurmforge/plans/stage.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("class StageInstanceLineage", plan_text)
        self.assertIn("lineage: StageInstanceLineage", plan_text)
        self.assertNotIn("lineage: dict[str, Any]", plan_text)

        violations = [
            str(path)
            for path in sorted(Path("src/slurmforge").rglob("*.py"))
            if "lineage.get(" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_input_binding_contract_is_not_semistructured(self) -> None:
        contracts_text = Path("src/slurmforge/contracts/inputs.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("class InputResolution", contracts_text)
        self.assertIn("required: bool = False", contracts_text)
        self.assertIn("inject: InputInjection", contracts_text)
        self.assertIn("resolution: InputResolution", contracts_text)
        self.assertNotIn("inject: JsonObject", contracts_text)
        self.assertNotIn("resolution: JsonObject", contracts_text)

        forbidden = (
            "binding.inject.get(",
            "binding.resolution.get(",
            "dict(binding.resolution",
        )
        violations = [
            f"{path}: {pattern}"
            for path in sorted(Path("src/slurmforge").rglob("*.py"))
            for pattern in forbidden
            if pattern in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(violations, [])

    def test_notification_plan_read_model_is_typed(self) -> None:
        files = (
            "src/slurmforge/root_model/models.py",
            "src/slurmforge/root_model/notifications.py",
            "src/slurmforge/notifications/read_model.py",
            "src/slurmforge/notifications/policy.py",
        )
        for path in files:
            text = Path(path).read_text(encoding="utf-8")
            self.assertNotIn("notification_plan: Any", text)
            self.assertNotIn("notification_plan_for_root(root: Path) -> Any", text)
            self.assertNotIn("notification_plan, event", text)

    def test_emit_reuses_notification_policy(self) -> None:
        emit_text = Path("src/slurmforge/emit/stage.py").read_text(encoding="utf-8")
        self.assertIn("email_notification_enabled", emit_text)
        self.assertNotIn("def _notification_enabled", emit_text)
        self.assertNotIn("batch.notification_plan.email", emit_text)

    def test_resolved_runtime_types_do_not_use_any_placeholders(self) -> None:
        checks = {
            "src/slurmforge/orchestration/status_read_model.py": (
                "tuple[Any, ...]",
                "from typing import Any",
            ),
            "src/slurmforge/submission/dependency_tree.py": (
                "client: Any",
                "from typing import Any",
            ),
            "src/slurmforge/orchestration/stage_build.py": (
                "tuple[Any, ...]",
                "from typing import Any",
            ),
            "src/slurmforge/cli/builders.py": (
                "tuple[Any, ...]",
                "from typing import Any",
            ),
        }
        for path, forbidden in checks.items():
            text = Path(path).read_text(encoding="utf-8")
            for pattern in forbidden:
                self.assertNotIn(pattern, text)
