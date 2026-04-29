from __future__ import annotations

from collections import Counter
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class ConfigSchemaCoverageTests(StageBatchSystemTestCase):
    def test_schema_has_no_duplicate_or_removed_paths(self) -> None:
        from slurmforge.config_schema import all_fields

        paths = [field.path for field in all_fields()]
        duplicates = sorted({path for path in paths if paths.count(path) > 1})
        self.assertEqual(duplicates, [])
        self.assertNotIn("orchestration.controller.resources", paths)
        self.assertNotIn("runtime.user.default.python.bin", paths)
        self.assertNotIn("environments.default.modules", paths)
        self.assertNotIn("stages.*.launcher.master_port", paths)

    def test_generated_config_reference_lists_every_schema_field(self) -> None:
        from slurmforge.config_schema import all_fields

        config_doc = Path("docs/config.md").read_text(encoding="utf-8")
        missing = [
            field.path for field in all_fields() if f"`{field.path}`" not in config_doc
        ]
        self.assertEqual(missing, [])

    def test_schema_carries_required_and_enum_contracts(self) -> None:
        from slurmforge.config_schema import all_fields
        from slurmforge.config_contract.options import OPTIONS_BY_PATH

        by_path = {field.path: field for field in all_fields()}
        required_counts = Counter(field.required for field in all_fields())

        self.assertGreater(required_counts[True], 0)
        self.assertGreater(required_counts[False], 0)
        self.assertLess(required_counts[None], len(by_path) // 3)
        self.assertTrue(by_path["project"].required)
        self.assertTrue(by_path["storage.root"].required)
        self.assertTrue(by_path["stages.*.outputs.*.kind"].required)
        self.assertIsNone(by_path["runs.axes"].required)
        self.assertIsNone(by_path["stages.*.entry.script"].required)
        self.assertEqual(
            [option.value for option in by_path["runs.type"].options],
            ["single", "grid", "cases", "matrix"],
        )
        self.assertEqual(
            [option.value for option in by_path["stages.*.launcher.type"].options],
            ["single", "python", "torchrun", "srun", "mpirun", "command"],
        )
        enum_mismatches = {
            field.path: field.options
            for field in all_fields()
            if field.options and field.options != OPTIONS_BY_PATH[field.path]
        }
        self.assertEqual(enum_mismatches, {})

    def test_key_registry_exposes_current_config_surface(self) -> None:
        from slurmforge.config_schema import (
            allowed_keys,
            allowed_stage_keys,
            allowed_top_level_keys,
            is_dynamic_parent,
        )

        self.assertEqual(
            allowed_top_level_keys(),
            {
                "artifact_store",
                "dispatch",
                "environments",
                "experiment",
                "hardware",
                "notifications",
                "orchestration",
                "project",
                "runs",
                "runtime",
                "sizing",
                "stages",
                "storage",
            },
        )
        self.assertEqual(allowed_stage_keys(), {"train", "eval"})
        self.assertEqual(allowed_keys("storage"), {"root"})
        self.assertEqual(
            allowed_keys("artifact_store"),
            {"strategy", "fallback_strategy", "verify_digest", "fail_on_verify_error"},
        )
        self.assertEqual(
            allowed_keys("stages.train.launcher"),
            {
                "type",
                "mode",
                "nnodes",
                "nproc_per_node",
                "rendezvous",
                "args",
                "srun_args",
            },
        )
        self.assertEqual(
            allowed_keys("stages.train.launcher.rendezvous"),
            {"backend", "endpoint", "port"},
        )
        self.assertEqual(
            allowed_keys("stages.train.inputs.checkpoint"),
            {"source", "expects", "required", "inject"},
        )
        self.assertEqual(
            allowed_keys("stages.train.outputs.checkpoint"),
            {"kind", "required", "discover", "file", "json_path"},
        )
        self.assertEqual(
            allowed_keys("stages.train.outputs.checkpoint.discover"),
            {"globs", "select"},
        )
        self.assertTrue(is_dynamic_parent("runs.axes"))
        self.assertTrue(is_dynamic_parent("stages.train.entry.args"))

    def test_key_registry_has_no_raw_list_item_children(self) -> None:
        from slurmforge.config_schema import allowed_keys

        self.assertNotIn("before[]", allowed_keys("stages.train"))
        self.assertNotIn("cases[]", allowed_keys("runs"))
