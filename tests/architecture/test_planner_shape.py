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
