from __future__ import annotations

import ast
from pathlib import Path

from tests.architecture.helpers import absolute_import_module
from tests.support.case import StageBatchSystemTestCase


class InternalFacadeImportTests(StageBatchSystemTestCase):
    def test_internal_facades_are_not_imported_directly(self) -> None:
        blocked = {
            "slurmforge.cli",
            "slurmforge.control",
            "slurmforge.emit",
            "slurmforge.executor",
            "slurmforge.inputs",
            "slurmforge.lineage",
            "slurmforge.materialization",
            "slurmforge.notifications",
            "slurmforge.orchestration",
            "slurmforge.outputs",
            "slurmforge.planner",
            "slurmforge.plans",
            "slurmforge.resolver",
            "slurmforge.root_model",
            "slurmforge.runtime",
            "slurmforge.sizing",
            "slurmforge.status",
            "slurmforge.storage",
            "slurmforge.submission",
        }
        violations: list[str] = []
        checked = [*Path("src/slurmforge").rglob("*.py"), *Path("tests").rglob("*.py")]
        for path in sorted(checked):
            if path == Path("tests/architecture/test_internal_facade_imports.py"):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module = absolute_import_module(path, node)
                if module in blocked:
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])
