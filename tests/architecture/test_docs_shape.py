from __future__ import annotations

import re
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

    def test_internals_document_config_contract_ownership(self) -> None:
        internals = Path("docs/internals.md").read_text(encoding="utf-8")
        self.assertIn("`config_contract` is the source", internals)
        self.assertIn("key registration", internals)
        self.assertIn("`docs_render.config_reference` owns", internals)
        self.assertNotIn("`config_schema` owns", internals)
        self.assertNotIn(
            "contract parsing, and spec-facing validation messages",
            internals,
        )

    def test_docs_use_current_stage_instance_id_format(self) -> None:
        docs_text = "\n".join(
            path.read_text(encoding="utf-8") for path in Path("docs").rglob("*.md")
        )
        self.assertNotIn("<run_id>.<stage_name>", docs_text)
        self.assertNotIn("run_001.train", docs_text)
        self.assertNotIn("run_001.eval", docs_text)

    def test_starter_protocol_literals_in_docs_are_generated(self) -> None:
        docs_text = "\n".join(
            _strip_generated_blocks(path.read_text(encoding="utf-8"))
            for path in Path("docs").rglob("*.md")
        )
        violations = [
            literal
            for literal in (
                "checkpoint_path",
                "SFORGE_INPUT_CHECKPOINT",
                "eval/metrics.json",
                "checkpoints/**/*.pt",
            )
            if literal in docs_text
        ]
        self.assertEqual(violations, [])

    def test_docs_rendering_is_split_by_document(self) -> None:
        old_tool = Path("tools/render_config_docs.py")
        new_tool = Path("tools/render_docs.py")
        render_root = Path("src/slurmforge/docs_render")
        self.assertFalse(old_tool.exists())
        self.assertTrue(new_tool.exists())
        self.assertLess(len(new_tool.read_text(encoding="utf-8").splitlines()), 80)
        self.assertNotIn("starter_io", new_tool.read_text(encoding="utf-8"))
        for name in (
            "__init__.py",
            "config_doc.py",
            "config_reference.py",
            "markers.py",
            "quickstart.py",
            "submission.py",
        ):
            self.assertTrue((render_root / name).exists())
        self.assertIn(
            "config_contract.starter_io",
            (render_root / "quickstart.py").read_text(encoding="utf-8"),
        )
        self.assertIn(
            "render_submission_binding_json",
            (render_root / "submission.py").read_text(encoding="utf-8"),
        )

    def test_docs_marker_replace_helper_has_single_owner(self) -> None:
        definitions: list[str] = []
        for path in sorted([Path("tools/render_docs.py"), *Path("src").rglob("*.py")]):
            text = path.read_text(encoding="utf-8")
            if "def replace_between_markers" in text:
                definitions.append(str(path))
        self.assertEqual(definitions, ["src/slurmforge/docs_render/markers.py"])

    def test_config_docs_field_reference_matches_catalog(self) -> None:
        from slurmforge.docs_render.config_reference import (
            render_global_field_reference,
        )
        from slurmforge.starter.config_examples import render_advanced_example

        config_doc = Path("docs/config.md").read_text(encoding="utf-8")
        self.assertIn("<!-- CONFIG_STARTER_EXAMPLE_START -->", config_doc)
        self.assertIn("<!-- CONFIG_STARTER_EXAMPLE_END -->", config_doc)
        self.assertIn("<!-- CONFIG_ADVANCED_EXAMPLE_START -->", config_doc)
        self.assertIn("<!-- CONFIG_ADVANCED_EXAMPLE_END -->", config_doc)
        self.assertIn("# Starter template: train-eval", config_doc)
        self.assertIn(render_advanced_example(), config_doc)
        self.assertIn("<!-- CONFIG_CONTRACT_REFERENCE_START -->", config_doc)
        self.assertIn("<!-- CONFIG_CONTRACT_REFERENCE_END -->", config_doc)
        self.assertIn(render_global_field_reference(), config_doc)

    def test_config_docs_generated_reference_is_current(self) -> None:
        result = subprocess.run(
            [sys.executable, "tools/render_docs.py", "--check"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


def _strip_generated_blocks(text: str) -> str:
    marker_pairs = (
        ("CONFIG_STARTER_EXAMPLE",),
        ("CONFIG_ADVANCED_EXAMPLE",),
        ("CONFIG_CONTRACT_REFERENCE",),
        ("QUICKSTART_STARTER_CONTRACT",),
        ("SUBMISSION_BINDING_JSON",),
        ("SUBMISSION_INPUT_YAML",),
    )
    stripped = text
    for (name,) in marker_pairs:
        stripped = re.sub(
            rf"<!-- {name}_START -->.*?<!-- {name}_END -->",
            "",
            stripped,
            flags=re.DOTALL,
        )
    return stripped
