from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class DocsShapeTests(StageBatchSystemTestCase):
    def test_run_record_contract_is_split_into_topic_docs(self) -> None:
        record_index = Path("docs/record-contract.md").read_text(encoding="utf-8")
        self.assertLess(len(record_index.splitlines()), 20)
        for name in (
            "artifacts.md",
            "planning.md",
            "runtime.md",
            "status.md",
            "submission.md",
        ):
            self.assertTrue(Path("docs/records", name).exists())

    def test_docs_do_not_reference_planner_core_facade(self) -> None:
        planner_core_facade = ".".join(("planner", "core"))
        docs_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in [Path("README.md"), *Path("docs").rglob("*.md")]
        )
        self.assertNotIn(planner_core_facade, docs_text)

    def test_config_docs_field_reference_matches_catalog(self) -> None:
        from slurmforge.config_schema import render_global_field_reference
        from slurmforge.starter.config_examples import render_advanced_example

        config_doc = Path("docs/config.md").read_text(encoding="utf-8")
        self.assertIn("<!-- CONFIG_STARTER_EXAMPLE_START -->", config_doc)
        self.assertIn("<!-- CONFIG_STARTER_EXAMPLE_END -->", config_doc)
        self.assertIn("<!-- CONFIG_ADVANCED_EXAMPLE_START -->", config_doc)
        self.assertIn("<!-- CONFIG_ADVANCED_EXAMPLE_END -->", config_doc)
        self.assertIn("# Starter template: train-eval", config_doc)
        self.assertIn(render_advanced_example(), config_doc)
        self.assertIn("<!-- CONFIG_SCHEMA_REFERENCE_START -->", config_doc)
        self.assertIn("<!-- CONFIG_SCHEMA_REFERENCE_END -->", config_doc)
        self.assertIn(render_global_field_reference(), config_doc)

    def test_config_docs_generated_reference_is_current(self) -> None:
        result = subprocess.run(
            [sys.executable, "tools/render_config_docs.py", "--check"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
