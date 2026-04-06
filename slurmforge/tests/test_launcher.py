from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch



class LauncherTests(unittest.TestCase):
    def test_generate_rejects_unsupported_output_dispatch_mode_field(self) -> None:
        from slurmforge.cli import generate

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            report = SimpleNamespace()
            with patch(
                "slurmforge.cli.generate.compile_source",
                return_value=report,
            ):
                with patch("slurmforge.cli.generate.iter_compile_report_lines", return_value=()):
                    with patch(
                        "slurmforge.cli.generate.require_success",
                        side_effect=ValueError("output contains unsupported keys: ['dispatch_mode']"),
                    ):
                        with self.assertRaisesRegex(ValueError, "dispatch_mode"):
                            generate.render_generate(
                                config_path=cfg_path,
                                cli_overrides=[],
                                dry_run=True,
                                project_root_override=None,
                            )

    def test_generate_plans_from_canonical_batch_spec(self) -> None:
        from slurmforge.cli import generate

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            planned_batch = SimpleNamespace(
                total_runs=3,
                sbatch_dir=Path(tmp) / "runs" / "demo" / "exp" / "batch_auto" / "sbatch",
            )
            report = SimpleNamespace()
            with patch("slurmforge.cli.generate.compile_source", return_value=report) as compile_mock:
                with patch("slurmforge.cli.generate.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.generate.require_success", return_value=planned_batch):
                        with patch("slurmforge.cli.generate.materialize_or_print_batch", return_value=None) as materialize_mock:
                            generate.render_generate(
                                config_path=cfg_path,
                                cli_overrides=[],
                                dry_run=True,
                                project_root_override=None,
                            )
        source = compile_mock.call_args.args[0]
        self.assertEqual(source.config_path, cfg_path.resolve())
        self.assertEqual(source.cli_overrides, ())
        self.assertIsNone(source.project_root)
        self.assertEqual(materialize_mock.call_args.kwargs["planned_batch"], planned_batch)

    def test_generate_passes_notify_config_to_materializer(self) -> None:
        from slurmforge.cli import generate

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            planned_batch = SimpleNamespace(
                total_runs=1,
                notify_cfg=SimpleNamespace(enabled=True, email="you@example.com", when="afterok"),
                sbatch_dir=Path(tmp) / "runs" / "demo" / "exp" / "batch_auto" / "sbatch",
            )
            report = SimpleNamespace()
            with patch("slurmforge.cli.generate.compile_source", return_value=report):
                with patch("slurmforge.cli.generate.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.generate.require_success", return_value=planned_batch):
                        with patch("slurmforge.cli.generate.materialize_or_print_batch", return_value=None) as materialize_mock:
                            generate.render_generate(
                                config_path=cfg_path,
                                cli_overrides=[],
                                dry_run=False,
                                project_root_override=None,
                            )

        self.assertEqual(materialize_mock.call_args.kwargs["planned_batch"], planned_batch)

    def test_generate_defaults_project_root_to_config_directory(self) -> None:
        from slurmforge.cli import generate

        with tempfile.TemporaryDirectory() as tmp:
            cfg_dir = Path(tmp) / "configs"
            cfg_dir.mkdir(parents=True, exist_ok=True)
            cfg_path = cfg_dir / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            planned_batch = SimpleNamespace(
                total_runs=1,
                sbatch_dir=cfg_dir / "runs" / "demo" / "exp" / "batch_auto" / "sbatch",
            )
            report = SimpleNamespace()
            with patch("slurmforge.cli.generate.compile_source", return_value=report) as compile_mock:
                with patch("slurmforge.cli.generate.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.generate.require_success", return_value=planned_batch):
                        with patch("slurmforge.cli.generate.materialize_or_print_batch", return_value=None):
                            generate.render_generate(
                                config_path=cfg_path,
                                cli_overrides=[],
                                dry_run=False,
                                project_root_override=None,
                            )

        self.assertIsNone(compile_mock.call_args.args[0].project_root)

    def test_validate_uses_planning_dry_run_with_project_root_override(self) -> None:
        from slurmforge.cli import validate

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg_path = tmp_path / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            output = io.StringIO()
            report = SimpleNamespace(
                has_failures=False,
                total_runs=1,
                warning_count=0,
                identity=SimpleNamespace(project_root=(tmp_path / "root_override").resolve()),
            )
            with patch("slurmforge.cli.validate.compile_source", return_value=report) as compile_mock:
                with patch("slurmforge.cli.validate.iter_compile_report_lines", return_value=()):
                    with redirect_stdout(output):
                        validate.render_validate(
                            config_path=cfg_path,
                            cli_overrides=["run.args.lr=0.001"],
                            project_root_override=str(tmp_path / "root_override"),
                            force=False,
                        )

        source = compile_mock.call_args.args[0]
        self.assertEqual(source.config_path, cfg_path.resolve())
        self.assertEqual(source.cli_overrides, ("run.args.lr=0.001",))
        self.assertEqual(source.project_root, tmp_path / "root_override")
        self.assertIn("[OK] Config is valid:", output.getvalue())

    def test_validate_reports_all_failed_runs_before_raising(self) -> None:
        from slurmforge.cli import validate
        from slurmforge.pipeline.compiler import BatchCompileError
        from slurmforge.pipeline.planning.contracts import PlanDiagnostic

        class _Failure:
            def __init__(self, run_index, total_runs, model_name, train_mode, diagnostics, phase="planning"):
                self.run_index = run_index
                self.total_runs = total_runs
                self.model_name = model_name
                self.train_mode = train_mode
                self.diagnostics = diagnostics
                self.phase = phase

        class _Report:
            def __init__(self):
                self.failed_runs = [
                    _Failure(
                        1,
                        2,
                        "m1",
                        "model_cli",
                        (
                            PlanDiagnostic(
                                severity="error",
                                category="topology",
                                code="runtime_processes_exceed_gpus",
                                message="bad topology",
                                stage="train",
                            ),
                        ),
                    ),
                    _Failure(
                        2,
                        2,
                        "m2",
                        "command",
                        (
                            PlanDiagnostic(
                                severity="error",
                                category="planning",
                                code="missing_input",
                                message="missing script",
                                stage="run",
                            ),
                        ),
                    ),
                ]
                self.failure_diagnostics = tuple(
                    diagnostic for failure in self.failed_runs for diagnostic in failure.diagnostics
                )
                self.total_failed_runs = len(self.failed_runs)
                self.total_runs = 2
                self.successful_runs = []
                self.has_failures = True
                self.batch_diagnostics = ()
                self.diagnostics = self.failure_diagnostics

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            output = io.StringIO()
            report = _Report()
            with patch("slurmforge.cli.validate.compile_source", return_value=report):
                with patch(
                    "slurmforge.cli.validate.iter_compile_report_lines",
                    return_value=(
                        "[ERROR] run 1/2 phase=planning model=m1 train_mode=model_cli",
                        "ERROR train:runtime_processes_exceed_gpus bad topology",
                        "[ERROR] run 2/2 phase=planning model=m2 train_mode=command",
                        "ERROR run:missing_input missing script",
                    ),
                ):
                    with redirect_stdout(output):
                        with self.assertRaises(BatchCompileError):
                            validate.render_validate(
                                config_path=cfg_path,
                                cli_overrides=[],
                                project_root_override=None,
                                force=False,
                            )

        rendered = output.getvalue()
        self.assertIn("[ERROR] run 1/2 phase=planning model=m1 train_mode=model_cli", rendered)
        self.assertIn("[ERROR] run 2/2 phase=planning model=m2 train_mode=command", rendered)
        self.assertIn("bad topology", rendered)
        self.assertIn("missing script", rendered)

    def test_replay_uses_canonical_replay_builder(self) -> None:
        from slurmforge.cli import replay

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            run_dir = tmp_path / "batch_src" / "runs" / "run_001_r1"
            planned_batch = SimpleNamespace(
                total_runs=1,
                sbatch_dir=tmp_path / "replay_runs" / "demo" / "exp" / "batch_replay" / "sbatch",
            )
            output = io.StringIO()
            report = SimpleNamespace(total_runs=1)
            with patch("slurmforge.cli.replay.compile_source", return_value=report) as compile_mock:
                with patch("slurmforge.cli.replay.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.replay.require_success", return_value=planned_batch):
                        with patch("slurmforge.cli.replay.materialize_or_print_batch", return_value=None) as materialize_mock:
                            with redirect_stdout(output):
                                replay.render_replay(
                                    source_run_dir=run_dir,
                                    source_snapshot_path=None,
                                    source_batch_root=None,
                                    run_ids=[],
                                    run_indices=[],
                                    cli_overrides=["cluster.mem=128G"],
                                    dry_run=True,
                                    project_root_override=str(tmp_path / "override_root"),
                                )

        request = compile_mock.call_args.args[0]
        self.assertEqual(request.source_run_dir, run_dir)
        self.assertIsNone(request.source_snapshot_path)
        self.assertIsNone(request.source_batch_root)
        self.assertEqual(request.run_ids, ())
        self.assertEqual(request.run_indices, ())
        self.assertEqual(request.cli_overrides, ("cluster.mem=128G",))
        self.assertEqual(request.project_root, tmp_path / "override_root")
        self.assertTrue(str(request.default_batch_name).startswith("replay_"))
        self.assertEqual(materialize_mock.call_args.kwargs["planned_batch"], planned_batch)
        self.assertIn("[REPLAY] source=", output.getvalue())

    def test_rerun_mode_uses_retry_builder(self) -> None:
        from slurmforge.cli import rerun
        from slurmforge.pipeline.config.normalize import normalize_notify
        from slurmforge.pipeline.planning import PlannedRun
        from tests._support import sample_run_plan, sample_run_snapshot

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_batch = tmp_path / "batch_src"
            source_batch.mkdir(parents=True, exist_ok=True)
            retry_plan = PlannedRun(
                plan=sample_run_plan(run_dir=str(tmp_path / "batch_retry" / "runs" / "run_001_r1")),
                snapshot=sample_run_snapshot(),
            )
            planned_batch = SimpleNamespace(
                total_runs=1,
                batch_root=tmp_path / "retry_runs" / "demo" / "exp" / "batch_retry_123",
                sbatch_dir=tmp_path / "retry_runs" / "demo" / "exp" / "batch_retry_123" / "sbatch",
                notify_cfg=normalize_notify({"enabled": False}),
                submit_dependencies={},
                manifest_extras={"retry_source": {"source_batch_root": str(source_batch)}},
            )
            report = SimpleNamespace(total_runs=1)
            with patch(
                "slurmforge.cli.rerun.compile_source",
                return_value=report,
            ) as retry_mock:
                with patch("slurmforge.cli.rerun.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.rerun.require_success", return_value=planned_batch):
                        with patch("slurmforge.cli.rerun.materialize_or_print_batch", return_value=None) as materialize_mock:
                            rerun.render_rerun(
                                source_batch_root=source_batch,
                                cli_overrides=["cluster.mem=128G"],
                                dry_run=True,
                                project_root_override=str(tmp_path),
                                status_query="oom",
                            )

        self.assertTrue(retry_mock.called)
        request = retry_mock.call_args.args[0]
        self.assertEqual(request.source_batch_root, source_batch)
        self.assertEqual(request.status_query, "oom")
        self.assertEqual(request.cli_overrides, ("cluster.mem=128G",))
        self.assertEqual(request.project_root, tmp_path)
        self.assertTrue(str(request.default_batch_name).startswith("retry_"))
        self.assertTrue(materialize_mock.called)

    def test_rerun_mode_uses_retry_batch_metadata_for_notify_and_batch_root(self) -> None:
        from slurmforge.cli import rerun
        from slurmforge.pipeline.config.normalize import normalize_notify
        from slurmforge.pipeline.planning import PlannedRun
        from tests._support import sample_run_plan, sample_run_snapshot

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_batch = tmp_path / "batch_src"
            source_batch.mkdir(parents=True, exist_ok=True)
            retry_plan = PlannedRun(
                plan=sample_run_plan(run_dir=str(tmp_path / "batch_retry" / "runs" / "run_001_r1")),
                snapshot=sample_run_snapshot(),
            )
            planned_batch = SimpleNamespace(
                total_runs=1,
                batch_root=tmp_path / "retry_runs" / "demo_retry" / "exp_retry" / "batch_retry_456",
                sbatch_dir=tmp_path / "retry_runs" / "demo_retry" / "exp_retry" / "batch_retry_456" / "sbatch",
                notify_cfg=normalize_notify({"enabled": True, "email": "you@example.com", "when": "afterany"}),
                submit_dependencies={"afterok": ["123"]},
                manifest_extras={"retry_source": {"source_batch_root": str(source_batch)}},
            )
            report = SimpleNamespace(total_runs=1)
            with patch(
                "slurmforge.cli.rerun.compile_source",
                return_value=report,
            ):
                with patch("slurmforge.cli.rerun.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.rerun.require_success", return_value=planned_batch):
                        with patch("slurmforge.cli.rerun.materialize_or_print_batch", return_value=None) as materialize_mock:
                            rerun.render_rerun(
                                source_batch_root=source_batch,
                                cli_overrides=[],
                                dry_run=False,
                                project_root_override=str(tmp_path),
                                status_query="failed",
                            )

        self.assertEqual(
            materialize_mock.call_args.kwargs["planned_batch"],
            planned_batch,
        )

    def test_rerun_mode_passes_retry_metadata_to_manifest(self) -> None:
        from slurmforge.cli import rerun
        from slurmforge.pipeline.config.normalize import normalize_notify
        from slurmforge.pipeline.planning import PlannedRun
        from tests._support import sample_run_plan, sample_run_snapshot

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_batch = tmp_path / "batch_src"
            source_batch.mkdir(parents=True, exist_ok=True)
            retry_plan = PlannedRun(
                plan=sample_run_plan(run_dir=str(tmp_path / "batch_retry" / "runs" / "run_001_r1")),
                snapshot=sample_run_snapshot(),
            )
            planned_batch = SimpleNamespace(
                total_runs=1,
                batch_root=tmp_path / "retry_runs" / "demo" / "exp" / "batch_retry_789",
                sbatch_dir=tmp_path / "retry_runs" / "demo" / "exp" / "batch_retry_789" / "sbatch",
                notify_cfg=normalize_notify({"enabled": False}),
                submit_dependencies={},
                manifest_extras={"retry_source": {"source_batch_root": str(source_batch), "status_query": "failed"}},
            )
            report = SimpleNamespace(total_runs=1)
            with patch("slurmforge.cli.rerun.compile_source", return_value=report):
                with patch("slurmforge.cli.rerun.iter_compile_report_lines", return_value=()):
                    with patch("slurmforge.cli.rerun.require_success", return_value=planned_batch):
                        with patch(
                            "slurmforge.cli.rerun.materialize_or_print_batch",
                            return_value=SimpleNamespace(
                                array_groups_meta=[],
                                submit_script=tmp_path / "submit.sh",
                                manifest_path=tmp_path / "batch_manifest.json",
                            ),
                        ) as materialize_mock:
                            rerun.render_rerun(
                                source_batch_root=source_batch,
                                cli_overrides=[],
                                dry_run=False,
                                project_root_override=str(tmp_path),
                                status_query="failed",
                            )

        self.assertEqual(materialize_mock.call_args.kwargs["planned_batch"], planned_batch)

    def test_main_dispatches_generate_subcommand(self) -> None:
        from slurmforge import launcher

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg_path = tmp_path / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            with patch("slurmforge.cli.generate.handle_generate", return_value=None) as render_mock:
                launcher.main(["generate", "--config", str(cfg_path), "--project_root", tmp])

        self.assertEqual(render_mock.call_args.args[0].config, str(cfg_path))

    def test_main_dispatches_rerun_subcommand(self) -> None:
        from slurmforge import launcher

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_batch = tmp_path / "batch_src"
            source_batch.mkdir(parents=True, exist_ok=True)
            with patch("slurmforge.cli.rerun.handle_rerun", return_value=None) as render_mock:
                launcher.main(["rerun", "--from", str(source_batch), "--status", "preempted", "--project_root", tmp])

        self.assertEqual(render_mock.call_args.args[0].source_batch_root, str(source_batch))
        self.assertEqual(render_mock.call_args.args[0].status, "preempted")

    def test_main_dispatches_validate_subcommand(self) -> None:
        from slurmforge import launcher

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg_path = tmp_path / "experiment.yaml"
            cfg_path.write_text("project: demo\n", encoding="utf-8")
            with patch("slurmforge.cli.validate.handle_validate", return_value=None) as render_mock:
                launcher.main(["validate", "--config", str(cfg_path), "--project_root", tmp])

        self.assertEqual(render_mock.call_args.args[0].config, str(cfg_path))

    def test_main_dispatches_replay_subcommand(self) -> None:
        from slurmforge import launcher

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            run_dir = tmp_path / "batch_src" / "runs" / "run_001_r1"
            run_dir.mkdir(parents=True, exist_ok=True)
            with patch("slurmforge.cli.replay.handle_replay", return_value=None) as render_mock:
                launcher.main(["replay", "--from-run", str(run_dir), "--project_root", tmp])

        self.assertEqual(render_mock.call_args.args[0].source_run_dir, str(run_dir))

    def test_main_dispatches_status_subcommand_all(self) -> None:
        from slurmforge import launcher

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            batch_root.mkdir(parents=True, exist_ok=True)
            with patch("slurmforge.cli.status.handle_status", return_value=None) as render_mock:
                launcher.main(["status", "--from", str(batch_root), "--status", "all"])

        self.assertEqual(render_mock.call_args.args[0].batch_root, str(batch_root))
        self.assertEqual(render_mock.call_args.args[0].status, "all")

    def test_render_status_prints_summary_and_filtered_rows(self) -> None:
        from slurmforge.cli import status
        from slurmforge.pipeline.status import ExecutionStatus
        from tests._support import sample_run_plan

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            batch_root.mkdir(parents=True, exist_ok=True)
            failed_plan = sample_run_plan(run_dir=str(batch_root / "runs" / "run_001"), run_id="r1")
            success_plan = sample_run_plan(run_dir=str(batch_root / "runs" / "run_002"), run_id="r2")
            statuses = [
                ExecutionStatus(
                    state="failed",
                    failure_class="oom",
                    failed_stage="train",
                    reason="cuda out of memory",
                    job_key="123",
                ),
                ExecutionStatus(
                    state="success",
                    reason="done",
                    job_key="124",
                ),
            ]
            output = io.StringIO()
            with patch("slurmforge.cli.status.load_batch_run_plans", return_value=[failed_plan, success_plan]):
                with patch("slurmforge.cli.status.load_or_infer_execution_status", side_effect=statuses):
                    with redirect_stdout(output):
                        status.render_status(batch_root=batch_root, status_query="failed")

        rendered = output.getvalue()
        self.assertIn("total_runs=2 matched=1 query=failed", rendered)
        self.assertIn("counts: oom=1, success=1", rendered)
        self.assertIn("run_001: state=failed class=oom stage=train job=123", rendered)
        self.assertNotIn("run_002: state=success", rendered)

    def test_main_dispatches_status_subcommand_filtered(self) -> None:
        from slurmforge import launcher

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            batch_root.mkdir(parents=True, exist_ok=True)
            with patch("slurmforge.cli.status.handle_status", return_value=None) as render_mock:
                launcher.main(["status", "--from", str(batch_root), "--status", "oom"])

        self.assertEqual(render_mock.call_args.args[0].batch_root, str(batch_root))
        self.assertEqual(render_mock.call_args.args[0].status, "oom")
