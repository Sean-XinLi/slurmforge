from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class OrchestrationShapeTests(StageBatchSystemTestCase):
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

    def test_control_runtime_owns_pipeline_gate_selection(self) -> None:
        control_root = Path("src/slurmforge/control")
        self.assertFalse((control_root / "materialization.py").exists())
        self.assertTrue((control_root / "workflow.py").exists())
