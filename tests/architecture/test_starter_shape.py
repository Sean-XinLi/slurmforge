from __future__ import annotations

import ast
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
            "script_render.py",
        ):
            self.assertTrue((template_root / name).exists())

    def test_advanced_config_example_is_structure_first(self) -> None:
        config_examples = Path("src/slurmforge/starter/config_examples.py")
        advanced = Path("src/slurmforge/starter/examples/advanced.py")
        render = Path("src/slurmforge/starter/examples/render.py")
        self.assertTrue(advanced.exists())
        self.assertTrue(render.exists())

        config_source = config_examples.read_text(encoding="utf-8")
        config_tree = ast.parse(config_source)
        config_functions = {
            node.name for node in config_tree.body if isinstance(node, ast.FunctionDef)
        }
        self.assertEqual(config_functions, {"render_starter_example"})
        self.assertNotIn("yaml.dump", config_source)
        self.assertNotIn("advanced_example_config", config_functions)

        advanced_source = advanced.read_text(encoding="utf-8")
        advanced_tree = ast.parse(advanced_source)
        advanced_functions = {
            node.name
            for node in advanced_tree.body
            if isinstance(node, ast.FunctionDef)
        }
        self.assertIn("advanced_example_config", advanced_functions)
        self.assertNotIn("dedent(", advanced_source)
        self.assertIn("base_config", advanced_source)
        self.assertIn("train_stage", advanced_source)
        self.assertIn("eval_stage_from_train", advanced_source)

    def test_starter_io_contract_values_have_single_source(self) -> None:
        constant_owner = Path("src/slurmforge/config_contract/starter_io.py")
        self.assertTrue(constant_owner.exists())
        for path in (
            Path("src/slurmforge/starter/templates/readme.py"),
            Path("src/slurmforge/starter/templates/script_render.py"),
            Path("src/slurmforge/starter/templates/stage_specs.py"),
            Path("src/slurmforge/config_contract/fields/stage_io_starter.py"),
        ):
            expected = (
                "from ..starter_io import"
                if "config_contract/fields" in str(path)
                else "config_contract.starter_io"
            )
            self.assertIn(
                expected,
                path.read_text(encoding="utf-8"),
            )

        raw_literals = (
            "checkpoint_path",
            "SFORGE_INPUT_CHECKPOINT",
            "eval/metrics.json",
            "checkpoints/**/*.pt",
        )
        checked = sorted(Path("src/slurmforge/starter").rglob("*.py")) + [
            Path("src/slurmforge/config_contract/fields/stage_io.py")
        ]
        violations: list[str] = []
        for path in checked:
            text = path.read_text(encoding="utf-8")
            for literal in raw_literals:
                if literal in text:
                    violations.append(f"{path} contains {literal}")
        self.assertEqual(violations, [])

    def test_stage_io_schema_fields_are_layered(self) -> None:
        stage_io = Path("src/slurmforge/config_contract/fields/stage_io.py")
        base = Path("src/slurmforge/config_contract/fields/stage_io_base.py")
        starter = Path("src/slurmforge/config_contract/fields/stage_io_starter.py")
        self.assertTrue(base.exists())
        self.assertTrue(starter.exists())
        self.assertFalse(
            Path("src/slurmforge/config_contract/fields/stage_io_generic.py").exists()
        )
        self.assertFalse(Path("src/slurmforge/config_schema").exists())

        self.assertIn("stage_io_base", stage_io.read_text(encoding="utf-8"))
        self.assertIn("stage_io_starter", stage_io.read_text(encoding="utf-8"))
        self.assertNotIn(
            "config_contract.starter_io",
            base.read_text(encoding="utf-8"),
        )
        self.assertIn(
            "from ..starter_io import",
            starter.read_text(encoding="utf-8"),
        )

    def test_starter_script_template_rendering_has_single_owner(self) -> None:
        owner = Path("src/slurmforge/starter/templates/script_render.py")
        self.assertTrue(owner.exists())
        owners = [
            path
            for path in sorted(Path("src/slurmforge/starter/templates").glob("*.py"))
            if "__SFORGE_" in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(owners, [owner])
