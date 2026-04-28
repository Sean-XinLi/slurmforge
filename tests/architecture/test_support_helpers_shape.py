from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class SupportHelpersShapeTests(StageBatchSystemTestCase):
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
        self.assertTrue(Path("tests/starter/helpers.py").exists())
        configs_text = Path("tests/helpers/configs.py").read_text(encoding="utf-8")
        self.assertNotIn("def _deep_merge", configs_text)
        self.assertNotIn("def _stage_batch_default_overlay", configs_text)
