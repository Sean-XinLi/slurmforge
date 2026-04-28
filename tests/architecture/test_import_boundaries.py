from __future__ import annotations

import ast

from tests.architecture.helpers import (
    absolute_import_module,
    find_cycles,
    inside_function,
    top_level_package_edges,
)
from tests.support.case import StageBatchSystemTestCase
from pathlib import Path


class ImportBoundaryTests(StageBatchSystemTestCase):
    def test_function_body_imports_do_not_cross_package_boundaries(self) -> None:
        root = Path("src/slurmforge")
        violations: list[str] = []
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            parents: dict[ast.AST, ast.AST] = {}
            for parent in ast.walk(tree):
                for child in ast.iter_child_nodes(parent):
                    parents[child] = parent
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                if not inside_function(node, parents):
                    continue
                if node.level > 1 or (
                    node.level == 0 and (node.module or "").startswith("slurmforge.")
                ):
                    violations.append(
                        f"{path}:{node.lineno} imports {'.' * node.level}{node.module or ''}"
                    )
        self.assertEqual(violations, [])

    def test_cli_does_not_directly_import_execution_layers(self) -> None:
        blocked = {
            "slurmforge.controller",
            "slurmforge.emit",
            "slurmforge.executor",
            "slurmforge.inputs",
            "slurmforge.lineage",
            "slurmforge.outputs",
            "slurmforge.planner",
            "slurmforge.plans",
            "slurmforge.resolver",
            "slurmforge.runtime",
            "slurmforge.slurm",
            "slurmforge.status",
            "slurmforge.storage",
            "slurmforge.submission",
        }
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/cli").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module = absolute_import_module(path, node)
                if any(
                    module == item or module.startswith(f"{item}.") for item in blocked
                ):
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_no_top_level_package_cycles(self) -> None:
        self.assertEqual(
            find_cycles(top_level_package_edges(Path("src/slurmforge"))), []
        )

    def test_contracts_package_is_leaf(self) -> None:
        blocked = {
            "slurmforge.controller",
            "slurmforge.emit",
            "slurmforge.executor",
            "slurmforge.inputs",
            "slurmforge.lineage",
            "slurmforge.notifications",
            "slurmforge.orchestration",
            "slurmforge.outputs",
            "slurmforge.planner",
            "slurmforge.plans",
            "slurmforge.resolver",
            "slurmforge.root_model",
            "slurmforge.runtime",
            "slurmforge.slurm",
            "slurmforge.spec",
            "slurmforge.status",
            "slurmforge.storage",
            "slurmforge.submission",
        }
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/contracts").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module = absolute_import_module(path, node)
                if any(
                    module == item or module.startswith(f"{item}.") for item in blocked
                ):
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_plans_do_not_import_spec_or_schema(self) -> None:
        blocked = {".".join(("slurmforge", "spec")), ".".join(("slurmforge", "schema"))}
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/plans").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module = absolute_import_module(path, node)
                if any(
                    module == item or module.startswith(f"{item}.") for item in blocked
                ):
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_storage_package_facade_and_paths_are_not_used_externally(self) -> None:
        violations: list[str] = []
        root = Path("src/slurmforge")
        for path in sorted(root.rglob("*.py")):
            if path == root / "storage" / "__init__.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module = absolute_import_module(path, node)
                if module == "slurmforge.storage":
                    violations.append(f"{path}:{node.lineno} imports storage facade")
                if (
                    module == "slurmforge.storage.paths"
                    and root / "storage" not in path.parents
                ):
                    violations.append(f"{path}:{node.lineno} imports storage paths")
        self.assertEqual(violations, [])
