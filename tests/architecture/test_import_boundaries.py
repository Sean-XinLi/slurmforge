from __future__ import annotations

import ast

from tests.support.case import StageBatchSystemTestCase
from tests.support.std import Path


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

    def test_removed_compatibility_entrypoints_stay_removed(self) -> None:
        import slurmforge.status as status
        import slurmforge.storage as storage

        self.assertFalse(hasattr(status, "batched_commits"))
        self.assertFalse(hasattr(storage, "load_stage_batch_plan"))
        self.assertFalse(hasattr(storage, "refresh_stage_batch_status"))
        self.assertFalse(Path("src/slurmforge/plans/loaders.py").exists())
        self.assertFalse(Path("src/slurmforge/storage/aggregate.py").exists())
        self.assertFalse(Path("src/slurmforge/storage/layout.py").exists())
        with self.assertRaises(ModuleNotFoundError):
            __import__("slurmforge.read_models")
        with self.assertRaises(ModuleNotFoundError):
            __import__(".".join(("slurmforge", "schema")))
        self.assertFalse(Path("src/slurmforge/spec/output_contract.py").exists())
        self.assertFalse(Path("src/slurmforge/schema").exists())

    def test_public_facades_do_not_export_internal_helpers(self) -> None:
        import slurmforge.notifications as notifications
        import slurmforge.plans as plans
        import slurmforge.planner as planner
        import slurmforge.resolver as resolver
        import slurmforge.spec as spec
        import slurmforge.submission as submission

        spec_internal = {
            "FileOutputDiscoveryRule",
            "OutputDiscoveryRule",
            "iter_run_overrides",
            "load_raw_config",
            "normalize_run_path",
            "parse_stage_output_contract",
            "run_id_for",
            "StageOutputContract",
            "StageOutputSpec",
        }
        plans_internal = {
            "group_plan_from_dict",
            "output_ref_from_dict",
            "prior_batch_lineage_to_dict",
            "stage_batch_plan_from_dict",
            "stage_instance_plan_from_dict",
            "stage_outputs_record_from_dict",
            "train_eval_pipeline_plan_from_dict",
        }
        submission_internal = {
            "GroupSubmissionRecord",
            "SubmissionLedger",
            "SubmitGeneration",
            "create_submit_generation",
            "dependency_for",
            "finalizer_dependency_group_ids",
            "load_ready_prepared_submission",
            "mark_stage_batch_queued",
            "read_submission_ledger",
            "write_submission_ledger",
        }
        planner_internal = {
            "expand_run_definitions",
            "stage_name_for_kind",
        }
        resolver_internal = {
            "stage_source_input_name",
        }
        notifications_internal = {
            "append_notification_event",
            "notification_record_path",
            "read_notification_record",
            "write_notification_record",
        }
        for name in spec_internal:
            self.assertFalse(hasattr(spec, name), name)
            self.assertNotIn(name, spec.__all__)
        for name in plans_internal:
            self.assertFalse(hasattr(plans, name), name)
            self.assertNotIn(name, plans.__all__)
        for name in submission_internal:
            self.assertFalse(hasattr(submission, name), name)
            self.assertNotIn(name, submission.__all__)
        for name in planner_internal:
            self.assertFalse(hasattr(planner, name), name)
            self.assertNotIn(name, planner.__all__)
        for name in resolver_internal:
            self.assertFalse(hasattr(resolver, name), name)
            self.assertNotIn(name, resolver.__all__)
        for name in notifications_internal:
            self.assertFalse(hasattr(notifications, name), name)
            self.assertNotIn(name, notifications.__all__)

    def test_storage_loader_no_longer_owns_root_read_model(self) -> None:
        import slurmforge.storage.loader as loader

        for name in (
            "collect_stage_statuses",
            "is_stage_batch_root",
            "is_train_eval_pipeline_root",
            "iter_stage_run_dirs",
        ):
            self.assertFalse(hasattr(loader, name), name)

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
                module = _absolute_import_module(path, node)
                if any(module == item or module.startswith(f"{item}.") for item in blocked):
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_plans_do_not_import_spec_or_removed_schema(self) -> None:
        blocked = {".".join(("slurmforge", "spec")), ".".join(("slurmforge", "schema"))}
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/plans").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.iter_child_nodes(tree):
                if not isinstance(node, ast.ImportFrom):
                    continue
                module = _absolute_import_module(path, node)
                if any(module == item or module.startswith(f"{item}.") for item in blocked):
                    violations.append(f"{path}:{node.lineno} imports {module}")
        self.assertEqual(violations, [])

    def test_orchestration_returns_data_instead_of_printing(self) -> None:
        violations: list[str] = []
        for path in sorted(Path("src/slurmforge/orchestration").rglob("*.py")):
            if "print(" in path.read_text(encoding="utf-8"):
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_orchestration_build_is_split_by_workflow(self) -> None:
        self.assertFalse(Path("src/slurmforge/orchestration/build.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/audit.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/estimate.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/pipeline_build.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/stage_build.py").exists())

    def test_orchestration_status_view_is_named_for_status_workflow(self) -> None:
        self.assertFalse(Path("src/slurmforge/orchestration/render.py").exists())
        self.assertTrue(Path("src/slurmforge/orchestration/status_view.py").exists())

    def test_output_discovery_is_split_by_output_kind(self) -> None:
        discovery_root = Path("src/slurmforge/outputs/discovery")
        self.assertFalse(Path("src/slurmforge/outputs/discovery.py").exists())
        self.assertTrue((discovery_root / "context.py").exists())
        self.assertTrue((discovery_root / "models.py").exists())
        self.assertTrue((discovery_root / "registry.py").exists())
        self.assertTrue((discovery_root / "service.py").exists())
        self.assertTrue((discovery_root / "writer.py").exists())
        for name in ("file.py", "files.py", "manifest.py", "metric.py"):
            self.assertTrue((discovery_root / "handlers" / name).exists())
        service_text = (discovery_root / "service.py").read_text(encoding="utf-8")
        self.assertNotIn("if output_cfg.kind ==", service_text)
        self.assertNotIn("elif output_cfg.kind ==", service_text)

    def test_cli_stage_common_is_split_by_concern(self) -> None:
        self.assertFalse(Path("src/slurmforge/cli/stage_common.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/args.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/builders.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/dry_run.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/render.py").exists())

    def test_notification_finalizer_runtime_is_not_submission_runtime(self) -> None:
        submission_finalizer = Path("src/slurmforge/submission/finalizer.py").read_text(encoding="utf-8")
        self.assertNotIn("deliver_notification", submission_finalizer)
        self.assertNotIn("load_notification_summary_input", submission_finalizer)
        self.assertNotIn("def run_finalizer", submission_finalizer)
        runtime = Path("src/slurmforge/notifications/finalizer_runtime.py").read_text(encoding="utf-8")
        self.assertIn("def run_finalizer", runtime)
        self.assertIn("deliver_notification", runtime)

    def test_storage_layout_is_split_by_root_type(self) -> None:
        self.assertFalse(Path("src/slurmforge/storage/layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/batch_layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/train_eval_pipeline_layout.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/status_seed.py").exists())
        self.assertTrue(Path("src/slurmforge/storage/controller_seed.py").exists())

    def test_sizing_package_stays_plan_agnostic(self) -> None:
        self.assertFalse(Path("src/slurmforge/sizing/estimate.py").exists())
        sizing_text = "\n".join(path.read_text(encoding="utf-8") for path in Path("src/slurmforge/sizing").rglob("*.py"))
        self.assertNotIn("hasattr(plan", sizing_text)
        self.assertNotIn("StageBatchPlan", sizing_text)
        self.assertNotIn("TrainEvalPipelinePlan", sizing_text)

    def test_spec_models_do_not_import_parser(self) -> None:
        text = Path("src/slurmforge/spec/models.py").read_text(encoding="utf-8")
        self.assertNotIn("parse_experiment_spec", text)
        self.assertNotIn("from .parser import", text)

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
                module = _absolute_import_module(path, node)
                if module == "slurmforge.storage":
                    violations.append(f"{path}:{node.lineno} imports storage facade")
                if module == "slurmforge.storage.paths" and root / "storage" not in path.parents:
                    violations.append(f"{path}:{node.lineno} imports storage paths")
        self.assertEqual(violations, [])

    def test_cli_flags_are_kebab_case_only(self) -> None:
        old_flags = ("--dry_run", "--emit_only", "--project_root")
        checked = list(Path("src/slurmforge").rglob("*.py")) + [
            Path("README.md"),
            Path("RUN_RECORD_CONTRACT.md"),
        ]
        violations: list[str] = []
        for path in checked:
            text = path.read_text(encoding="utf-8")
            for flag in old_flags:
                if flag in text:
                    violations.append(f"{path} contains {flag}")
        self.assertEqual(violations, [])


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
