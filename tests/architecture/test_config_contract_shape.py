from __future__ import annotations

import ast
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class ConfigContractShapeTests(StageBatchSystemTestCase):
    def test_config_field_catalog_has_single_contract_owner(self) -> None:
        contract_root = Path("src/slurmforge/config_contract")
        self.assertTrue((contract_root / "registry.py").exists())
        self.assertTrue((contract_root / "models.py").exists())
        self.assertTrue((contract_root / "fields").exists())
        self.assertFalse((contract_root / "defaults.py").exists())
        self.assertFalse((contract_root / "options.py").exists())
        self.assertFalse(Path("src/slurmforge/config_schema").exists())

        violations: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if module.endswith(
                        (
                            "config_contract.defaults",
                            "config_contract.options",
                            "config_schema",
                        )
                    ):
                        violations.append(f"{path}:{module}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.endswith(
                            (
                                "config_contract.defaults",
                                "config_contract.options",
                                "config_schema",
                            )
                        ):
                            violations.append(f"{path}:{alias.name}")
        self.assertEqual(violations, [])

    def test_config_contract_owns_field_queries_and_key_registry(self) -> None:
        contract_root = Path("src/slurmforge/config_contract")
        registry = (contract_root / "registry.py").read_text(encoding="utf-8")
        keys = (contract_root / "keys.py").read_text(encoding="utf-8")

        self.assertIn("def fields_for_template", registry)
        self.assertIn("def comment_for", registry)
        self.assertIn("def reject_unknown_config_keys", keys)
        self.assertIn("CONFIG_FIELDS", keys)

    def test_default_values_are_non_field_constants(self) -> None:
        allowed = {
            "DEFAULT_CONFIG_FILENAME",
            "DEFAULT_OUTPUT_DIR",
            "AUTO_VALUE",
            "DEFAULT_ENVIRONMENT_NAME",
            "DEFAULT_RUNTIME_NAME",
        }
        tree = ast.parse(
            Path("src/slurmforge/config_contract/default_values.py").read_text(
                encoding="utf-8"
            )
        )
        assigned = {
            target.id
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
            and (target.id == "AUTO_VALUE" or target.id.startswith("DEFAULT_"))
        }
        self.assertEqual(assigned, allowed)

    def test_contract_field_modules_own_field_defaults(self) -> None:
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/config_contract/fields").glob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "default_for(" in text:
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_spec_models_do_not_read_config_defaults(self) -> None:
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/spec/models").glob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "config_contract.registry" in text or "default_for(" in text:
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_plan_models_do_not_reexport_workflow_gates(self) -> None:
        text = Path("src/slurmforge/plans/train_eval.py").read_text(encoding="utf-8")
        for name in ("TRAIN_GROUP_GATE", "EVAL_SHARD_GATE", "FINAL_GATE"):
            self.assertNotIn(name, text)

    def test_control_and_stage_partition_defaults_are_explicit(self) -> None:
        from slurmforge.config_contract.registry import default_for, field_by_path

        self.assertIsNone(default_for("orchestration.control.partition"))
        self.assertEqual(default_for("stages.*.resources.partition"), "gpu")
        self.assertIsNone(
            field_by_path("orchestration.control.partition").default_value
        )
        self.assertEqual(
            field_by_path("stages.*.resources.partition").default_value, "gpu"
        )

    def test_starter_defaults_are_read_from_registry(self) -> None:
        checked = (
            Path("src/slurmforge/starter/templates/base.py"),
            Path("src/slurmforge/starter/templates/resources.py"),
            Path("src/slurmforge/starter/templates/stage_specs.py"),
        )
        for path in checked:
            text = path.read_text(encoding="utf-8")
            self.assertIn("default_for", text)
