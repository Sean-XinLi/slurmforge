from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class StarterShapeTests(StageBatchSystemTestCase):
    def test_starter_template_shared_builders_are_split_by_concern(self) -> None:
        template_root = Path("src/slurmforge/starter/templates")
        yaml_root = Path("src/slurmforge/starter/config_yaml")
        stage_yaml_root = yaml_root / "stages"
        self.assertFalse((template_root / "fragments.py").exists())
        self.assertFalse(
            (Path("src/slurmforge/starter") / ("config_yaml" + ".py")).exists()
        )
        self.assertFalse((yaml_root / "stages.py").exists())
        for name in ("__init__.py", "render.py", "scalar.py", "sections.py"):
            self.assertTrue((yaml_root / name).exists())
        for name in (
            "__init__.py",
            "build.py",
            "entry.py",
            "inputs.py",
            "outputs.py",
            "resources.py",
        ):
            self.assertTrue((stage_yaml_root / name).exists())
        for name in (
            "base.py",
            "resources.py",
            "stage_specs.py",
            "readme.py",
            "scripts.py",
        ):
            self.assertTrue((template_root / name).exists())
