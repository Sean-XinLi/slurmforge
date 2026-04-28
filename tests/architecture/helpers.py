from __future__ import annotations

import ast

from pathlib import Path


def inside_function(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current = parents.get(node)
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return True
        current = parents.get(current)
    return False


def absolute_import_module(path: Path, node: ast.ImportFrom) -> str:
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


def top_level_package_edges(root: Path) -> dict[str, set[str]]:
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
                modules.append(absolute_import_module(path, node))
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            for module in modules:
                target = _target_top_package(module)
                if target is not None and target != source:
                    edges[source].add(target)
                    edges.setdefault(target, set())
    return edges


def find_cycles(edges: dict[str, set[str]]) -> list[str]:
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
