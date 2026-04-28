from __future__ import annotations

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

    def test_import_linter_config_is_absent(self) -> None:
        self.assertFalse(Path(".importlinter").exists())
        self.assertNotIn("lint-imports", Path("README.md").read_text(encoding="utf-8"))

    def test_docs_do_not_reference_planner_core_facade(self) -> None:
        planner_core_facade = ".".join(("planner", "core"))
        docs_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in [Path("README.md"), *Path("docs").rglob("*.md")]
        )
        self.assertNotIn(planner_core_facade, docs_text)
