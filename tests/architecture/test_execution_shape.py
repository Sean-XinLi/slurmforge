from __future__ import annotations

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

    def test_train_eval_controller_runtime_is_split_by_responsibility(self) -> None:
        controller_root = Path("src/slurmforge/controller")
        self.assertTrue((controller_root / "stage_runtime.py").exists())
        self.assertTrue((controller_root / "terminal.py").exists())
        runtime_text = (controller_root / "train_eval_pipeline.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("def _wait_terminal", runtime_text)
        self.assertNotIn("deliver_notification", runtime_text)

    def test_notification_finalizer_runtime_is_not_submission_runtime(self) -> None:
        submission_finalizer = Path("src/slurmforge/submission/finalizer.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("deliver_notification", submission_finalizer)
        self.assertNotIn("load_notification_summary_input", submission_finalizer)
        self.assertNotIn("def run_finalizer", submission_finalizer)
        runtime = Path("src/slurmforge/notifications/finalizer_runtime.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("def run_finalizer", runtime)
        self.assertIn("deliver_notification", runtime)
