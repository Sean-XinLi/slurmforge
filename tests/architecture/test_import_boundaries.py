from __future__ import annotations

import ast
from pathlib import Path

from tests.architecture.helpers import (
    absolute_import_module,
    find_cycles,
    inside_function,
    top_level_package_edges,
)
from tests.support.case import StageBatchSystemTestCase


def _import_modules(path: Path, tree: ast.AST) -> list[tuple[int, str]]:
    modules: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            modules.append((node.lineno, absolute_import_module(path, node)))
        elif isinstance(node, ast.Import):
            modules.extend((node.lineno, alias.name) for alias in node.names)
    return modules


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
            "slurmforge.control",
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
            "slurmforge.control",
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

    def test_control_job_contract_is_leaf(self) -> None:
        allowed = {
            "__future__",
            "dataclasses",
            "typing",
            "slurmforge.errors",
            "slurmforge.record_fields",
        }
        path = Path("src/slurmforge/control_job_contract.py")
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations: list[str] = []
        for node in ast.iter_child_nodes(tree):
            modules: list[str] = []
            if isinstance(node, ast.ImportFrom):
                modules.append(absolute_import_module(path, node))
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            for module in modules:
                if module not in allowed:
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_runtime_code_does_not_import_legacy_workflow_enums(self) -> None:
        root = Path("src/slurmforge")
        self.assertFalse((root / "workflow_enums.py").exists())
        violations: list[str] = []
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for lineno, module in _import_modules(path, tree):
                if module == "slurmforge.workflow_enums":
                    violations.append(f"{path}:{lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_runtime_code_does_not_import_workflow_state_records_facade(self) -> None:
        root = Path("src/slurmforge")
        self.assertFalse((root / "storage" / "workflow_state_records.py").exists())
        violations: list[str] = []
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for lineno, module in _import_modules(path, tree):
                if module == "slurmforge.storage.workflow_state_records":
                    violations.append(f"{path}:{lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_import_module_scanner_includes_function_body_imports(self) -> None:
        tree = ast.parse(
            "\n".join(
                (
                    "def load():",
                    "    import slurmforge.workflow_enums",
                    "    from slurmforge.storage.workflow_state_records import WorkflowState",
                )
            ),
            filename="sample.py",
        )
        modules = _import_modules(Path("src/slurmforge/sample.py"), tree)

        self.assertIn((2, "slurmforge.workflow_enums"), modules)
        self.assertIn((3, "slurmforge.storage.workflow_state_records"), modules)

    def test_release_policy_values_have_single_contract_owner(self) -> None:
        root = Path("src/slurmforge")
        definitions: list[str] = []
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                targets: list[ast.expr] = []
                if isinstance(node, ast.Assign):
                    targets = list(node.targets)
                elif isinstance(node, ast.AnnAssign):
                    targets = [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and target.id == "RELEASE_POLICIES":
                        definitions.append(f"{path}:RELEASE_POLICIES")
        self.assertEqual(
            definitions,
            ["src/slurmforge/release_policy_contract.py:RELEASE_POLICIES"],
        )

        option_sets = ast.parse(
            (root / "config_contract" / "option_sets.py").read_text(encoding="utf-8")
        )
        assigned_names = {
            target.id
            for node in ast.iter_child_nodes(option_sets)
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        self.assertNotIn("RELEASE_POLICIES", assigned_names)
        self.assertIn("RELEASE_POLICY_OPTIONS", assigned_names)

    def test_record_contract_layers_do_not_import_config_schema(self) -> None:
        blocked = "slurmforge.config_schema"
        violations: list[str] = []
        for package in ("contracts", "sizing"):
            for path in sorted(Path("src/slurmforge", package).rglob("*.py")):
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                for node in ast.iter_child_nodes(tree):
                    if not isinstance(node, ast.ImportFrom):
                        continue
                    module = absolute_import_module(path, node)
                    if module == blocked or module.startswith(f"{blocked}."):
                        violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_output_selector_normalization_has_single_owner(self) -> None:
        definitions: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name in {
                    "_normalize_selector",
                    "normalize_output_selector",
                }:
                    definitions.append(f"{path}:{node.name}")

        self.assertEqual(
            definitions,
            [
                "src/slurmforge/contracts/output_selectors.py:normalize_output_selector"
            ],
        )

    def test_internal_code_imports_config_contract_not_legacy_defaults_facade(
        self,
    ) -> None:
        violations: list[str] = []
        root = Path("src/slurmforge")
        self.assertFalse((root / "defaults.py").exists())
        for path in sorted(root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                modules: list[str] = []
                if isinstance(node, ast.ImportFrom):
                    modules.append(absolute_import_module(path, node))
                elif isinstance(node, ast.Import):
                    modules.extend(alias.name for alias in node.names)
                for module in modules:
                    if module == "slurmforge.defaults":
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

    def test_train_eval_runtime_contract_has_single_constant_source(self) -> None:
        root = Path("src/slurmforge")
        self.assertTrue((root / "workflow_contract.py").exists())
        violations: list[str] = []
        for package in ("control", "orchestration", "planner", "storage"):
            for path in sorted((root / package).rglob("*.py")):
                text = path.read_text(encoding="utf-8")
                for value in (
                    '"pipeline_stage"',
                    '"pipeline_entry"',
                    '"streaming"',
                ):
                    if value in text:
                        violations.append(f"{path}:{value}")
        self.assertEqual(violations, [])
