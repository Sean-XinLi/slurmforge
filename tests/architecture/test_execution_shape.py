from __future__ import annotations

import ast
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class ExecutionShapeTests(StageBatchSystemTestCase):
    def test_output_discovery_is_split_by_output_kind(self) -> None:
        discovery_root = Path("src/slurmforge/outputs/discovery")
        self.assertFalse(Path("src/slurmforge/outputs/discovery.py").exists())
        for name in (
            "context.py",
            "models.py",
            "registry.py",
            "service.py",
            "writer.py",
        ):
            self.assertTrue((discovery_root / name).exists())
        for name in ("file.py", "files.py", "manifest.py", "metric.py"):
            self.assertTrue((discovery_root / "handlers" / name).exists())
        service_text = (discovery_root / "service.py").read_text(encoding="utf-8")
        self.assertNotIn("if output_cfg.kind ==", service_text)
        self.assertNotIn("elif output_cfg.kind ==", service_text)
        registry_text = (discovery_root / "registry.py").read_text(encoding="utf-8")
        self.assertIn("StageOutputSpec", registry_text)
        self.assertNotIn("Any", registry_text)
        for path in sorted((discovery_root / "handlers").glob("*.py")):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("output_cfg: Any", text)

    def test_stage_sbatch_rendering_is_split_by_render_surface(self) -> None:
        stage_render_root = Path("src/slurmforge/emit/stage_render")
        self.assertFalse(Path("src/slurmforge/emit/stage_render.py").exists())
        self.assertTrue(stage_render_root.is_dir())
        for name in ("__init__.py", "group.py", "headers.py", "notification.py"):
            self.assertTrue((stage_render_root / name).exists())

    def test_executor_attempt_transaction_is_split(self) -> None:
        self.assertTrue(Path("src/slurmforge/executor/attempt.py").exists())
        self.assertTrue(Path("src/slurmforge/executor/runner.py").exists())
        self.assertTrue(Path("src/slurmforge/executor/finalize.py").exists())
        stage_text = Path("src/slurmforge/executor/stage.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("subprocess.run", stage_text)
        self.assertNotIn("discover_stage_outputs", stage_text)
        self.assertNotIn("require_runtime_contract", stage_text)

    def test_slurm_fake_client_is_test_support_only(self) -> None:
        self.assertTrue(Path("src/slurmforge/slurm/parsers.py").exists())
        self.assertTrue(Path("src/slurmforge/slurm/protocol.py").exists())
        self.assertTrue(Path("tests/support/slurm.py").exists())
        test_fake_text = Path("tests/support/slurm.py").read_text(encoding="utf-8")
        inherited_fake = "class " + "FakeSlurmClient(SlurmClient)"
        self.assertNotIn(inherited_fake, test_fake_text)
        source_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in Path("src/slurmforge").rglob("*.py")
        )
        self.assertNotIn("FakeSlurmClient", source_text)

    def test_input_verification_records_are_split_by_responsibility(self) -> None:
        verification_root = Path("src/slurmforge/inputs/verification")
        self.assertFalse(Path("src/slurmforge/inputs/verification_records.py").exists())
        self.assertTrue(verification_root.is_dir())
        for name in ("__init__.py", "digests.py", "path_checks.py", "records.py"):
            self.assertTrue((verification_root / name).exists())

    def test_train_eval_control_runtime_is_split_by_responsibility(self) -> None:
        control_root = Path("src/slurmforge/control")
        self.assertFalse((control_root / "eval_shard.py").exists())
        for removed in (
            "auto_advance.py",
            "eval_materialization.py",
            "eval_selection.py",
            "eval_transition.py",
            "final_gate.py",
            "control_submissions.py",
            "stage_runtime.py",
            "train_group.py",
            "train_transition.py",
        ):
            self.assertFalse((control_root / removed).exists())
        for name in (
            "dependencies.py",
            "dispatch_budget.py",
            "dispatch_materialization.py",
            "dispatch_pack.py",
            "dispatch_queue.py",
            "dispatch_submit.py",
            "finalization.py",
            "control_submission_ledger.py",
            "control_submission_records.py",
            "control_submission_submit.py",
            "gates.py",
            "initial_prepare.py",
            "initial_submit.py",
            "instance_reconcile.py",
            "stage_submit.py",
            "terminal_notification.py",
            "workflow.py",
        ):
            self.assertTrue((control_root / name).exists())
        storage_root = Path("src/slurmforge/storage")
        for name in (
            "workflow_state_constants.py",
            "workflow_state_factory.py",
            "workflow_state_models.py",
            "workflow_state_mutations.py",
            "workflow_state_serde.py",
            "workflow_state_validation.py",
        ):
            self.assertTrue((storage_root / name).exists())
        self.assertFalse((storage_root / "workflow_state_records.py").exists())
        workflow_text = (control_root / "workflow.py").read_text(encoding="utf-8")
        self.assertNotIn("deliver_notification", workflow_text)
        self.assertNotIn("materialize_stage_batch", workflow_text)
        self.assertTrue(Path("src/slurmforge/control_job_contract.py").exists())
        self.assertTrue(Path("src/slurmforge/gate_task_map_contract.py").exists())
        self.assertTrue(Path("src/slurmforge/release_policy_contract.py").exists())
        self.assertFalse(Path("src/slurmforge/workflow_enums.py").exists())
        self.assertFalse(Path("src/slurmforge/contracts/control_jobs.py").exists())
        gate_runtime_text = (control_root / "gate_runtime.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("load_gate_task_map", gate_runtime_text)
        self.assertNotIn('payload["tasks"]', gate_runtime_text)
        self.assertNotIn("read_json(task_map", gate_runtime_text)
        deprecated_names = {
            "ControlSubmissionRecord",
            "WorkflowStatusControlJobRecord",
        }
        deprecated_usages: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                name = ""
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    name = node.name
                elif isinstance(node, ast.Name):
                    name = node.id
                elif isinstance(node, ast.Attribute):
                    name = node.attr
                elif isinstance(node, ast.alias):
                    name = node.asname or node.name
                if name in deprecated_names:
                    deprecated_usages.append(f"{path}:{node.lineno}:{name}")
        self.assertEqual(deprecated_usages, [])
        for path in sorted(control_root.glob("*.py")):
            if path.name in {
                "control_submission_records.py",
            }:
                continue
            text = path.read_text(encoding="utf-8")
            self.assertNotIn('state["', text, path.name)
            self.assertNotIn('record["', text, path.name)

    def test_status_view_is_split_into_read_model_and_formatter(self) -> None:
        orchestration_root = Path("src/slurmforge/orchestration")
        for name in ("status_read_model.py", "status_format.py", "status_view.py"):
            self.assertTrue((orchestration_root / name).exists())
        status_view = (orchestration_root / "status_view.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("read_workflow_status", status_view)
        self.assertNotIn("reconcile_root_submissions", status_view)
        self.assertNotIn('workflow_status["', status_view)
        self.assertNotIn("workflow_status.get", status_view)

    def test_notifications_use_slurm_mail_submission_runtime(self) -> None:
        submission_notifications = Path(
            "src/slurmforge/submission/notifications.py"
        ).read_text(
            encoding="utf-8"
        )
        self.assertNotIn("deliver_notification", submission_notifications)
        self.assertNotIn("load_notification_summary_input", submission_notifications)
        service = Path("src/slurmforge/submission/notification_mail.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("mail_user", service)
        self.assertIn("SlurmSubmitOptions", service)
        self.assertFalse(Path("src/slurmforge/notifications/finalizer_runtime.py").exists())

    def test_runtime_probe_uses_typed_runtime_plan(self) -> None:
        text = Path("src/slurmforge/runtime/probe.py").read_text(encoding="utf-8")
        self.assertIn("RuntimePlan", text)
        self.assertNotIn("runtime_plan: Any", text)
        self.assertNotIn("def _section", text)
        self.assertNotIn("def _field", text)
        self.assertNotIn("payload.get(", text)

    def test_dry_run_audit_uses_typed_validation_model(self) -> None:
        model_path = Path("src/slurmforge/planner/audit_models.py")
        self.assertTrue(model_path.exists())
        model_text = model_path.read_text(encoding="utf-8")
        audit_text = Path("src/slurmforge/planner/audit.py").read_text(
            encoding="utf-8"
        )
        cli_text = Path("src/slurmforge/cli/dry_run.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("class StageBatchDryRunValidation", model_text)
        self.assertIn("def dry_run_audit_to_dict", model_text)
        self.assertIn("StageBatchDryRunValidation", audit_text)
        self.assertIn("dry_run_audit_to_dict", cli_text)

        forbidden = (
            "item.get(",
            "report.get(",
            'validation["',
            'stage["',
            "to_jsonable(",
            "dict[str, Any]",
            "list[dict[str, Any]]",
        )
        violations = [
            pattern for pattern in forbidden if pattern in audit_text
        ]
        self.assertEqual(violations, [])
