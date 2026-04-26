from __future__ import annotations

import ast

from tests.support import *  # noqa: F401,F403


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
                if not _inside_function(node, parents):
                    continue
                if node.level > 1 or (node.level == 0 and (node.module or "").startswith("slurmforge.")):
                    violations.append(f"{path}:{node.lineno} imports {'.' * node.level}{node.module or ''}")
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
                module = _absolute_import_module(path, node)
                if any(module == item or module.startswith(f"{item}.") for item in blocked):
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_no_top_level_package_cycles(self) -> None:
        edges = _top_level_package_edges(Path("src/slurmforge"))
        cycles = _find_cycles(edges)
        self.assertEqual(cycles, [])


def _inside_function(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current = parents.get(node)
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return True
        current = parents.get(current)
    return False


def _absolute_import_module(path: Path, node: ast.ImportFrom) -> str:
    if node.level == 0:
        return node.module or ""
    package_parts = path.with_suffix("").parts
    try:
        root_index = package_parts.index("slurmforge")
    except ValueError:
        return node.module or ""
    current = list(package_parts[root_index:-1])
    if node.level > 1:
        current = current[: -(node.level - 1)]
    if node.module:
        current.extend(node.module.split("."))
    return ".".join(current)


def _source_top_package(root: Path, path: Path) -> str | None:
    relative = path.relative_to(root).with_suffix("")
    parts = relative.parts
    if len(parts) < 2:
        return None
    return parts[0]


def _target_top_package(module: str) -> str | None:
    parts = module.split(".")
    if len(parts) < 2 or parts[0] != "slurmforge":
        return None
    return parts[1]


def _top_level_package_edges(root: Path) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = {}
    for path in sorted(root.rglob("*.py")):
        source = _source_top_package(root, path)
        if source is None:
            continue
        edges.setdefault(source, set())
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.iter_child_nodes(tree):
            modules: list[str] = []
            if isinstance(node, ast.ImportFrom):
                modules.append(_absolute_import_module(path, node))
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            for module in modules:
                target = _target_top_package(module)
                if target is not None and target != source:
                    edges[source].add(target)
                    edges.setdefault(target, set())
    return edges


def _find_cycles(edges: dict[str, set[str]]) -> list[str]:
    visited: set[str] = set()
    active: list[str] = []
    active_set: set[str] = set()
    cycles: set[tuple[str, ...]] = set()

    def visit(node: str) -> None:
        if node in active_set:
            cycle = active[active.index(node) :] + [node]
            smallest = min(range(len(cycle) - 1), key=lambda index: cycle[index])
            rotated = cycle[smallest:-1] + cycle[:smallest] + [cycle[smallest]]
            cycles.add(tuple(rotated))
            return
        if node in visited:
            return
        visited.add(node)
        active.append(node)
        active_set.add(node)
        for target in sorted(edges.get(node, ())):
            visit(target)
        active_set.remove(node)
        active.pop()

    for node in sorted(edges):
        visit(node)
    return [" -> ".join(cycle) for cycle in sorted(cycles)]
