from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class ConfigContractShapeTests(StageBatchSystemTestCase):
    def test_config_field_catalog_has_single_contract_owner(self) -> None:
        contract_root = Path("src/slurmforge/config_contract")
        self.assertTrue((contract_root / "registry.py").exists())
        self.assertTrue((contract_root / "models.py").exists())
        self.assertTrue((contract_root / "fields").exists())

        schema_sections = Path("src/slurmforge/config_schema/sections")
        violations: list[str] = []
        for path in sorted(schema_sections.glob("*.py")):
            if path.name == "__init__.py":
                continue
            text = path.read_text(encoding="utf-8")
            for marker in ("ConfigField(", "default_value=", "options="):
                if marker in text:
                    violations.append(f"{path}:{marker}")
        self.assertEqual(violations, [])

    def test_defaults_and_options_are_registry_facades(self) -> None:
        defaults = Path("src/slurmforge/config_contract/defaults.py").read_text(
            encoding="utf-8"
        )
        options = Path("src/slurmforge/config_contract/options.py").read_text(
            encoding="utf-8"
        )
        schema_fields = Path("src/slurmforge/config_schema/fields.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("from .registry import default_for", defaults)
        self.assertIn("from .registry import", options)
        self.assertIn("config_contract.registry", schema_fields)
        self.assertNotIn(
            "CONFIG_FIELDS: Final[tuple[ConfigField, ...]] = (", schema_fields
        )

    def test_control_and_stage_partition_defaults_are_explicit(self) -> None:
        from slurmforge.config_contract.defaults import (
            DEFAULT_CONTROL_PARTITION,
            DEFAULT_STAGE_RESOURCES_PARTITION,
        )
        from slurmforge.config_contract.registry import field_by_path

        self.assertIsNone(DEFAULT_CONTROL_PARTITION)
        self.assertEqual(DEFAULT_STAGE_RESOURCES_PARTITION, "gpu")
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
