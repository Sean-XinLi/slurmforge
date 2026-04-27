from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    SchemaVersion,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
    write_stage_batch_layout,
)
from tests.support.std import Namespace, Path, io, json, redirect_stderr, redirect_stdout, tempfile, yaml


class CliTests(StageBatchSystemTestCase):
    def test_plan_subcommands_keep_eval_source_arguments_off_train(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        stderr = io.StringIO()
        with redirect_stdout(io.StringIO()), redirect_stderr(stderr), self.assertRaises(SystemExit):
            parser.parse_args(["plan", "train", "--config", "unused.yaml", "--checkpoint", "/tmp/checkpoint.pt"])
        self.assertIn("unrecognized arguments: --checkpoint", stderr.getvalue())

        args = parser.parse_args(["plan", "eval", "--config", "unused.yaml", "--checkpoint", "/tmp/checkpoint.pt"])
        self.assertEqual(args.command, "plan")
        self.assertEqual(args.plan_command, "eval")
        self.assertEqual(args.checkpoint, "/tmp/checkpoint.pt")

    def test_plan_dry_run_previews_and_default_emits(self) -> None:
        from slurmforge.cli.plan import handle_plan

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            args = Namespace(
                plan_command="train",
                config=str(cfg_path),
                set=[],
                project_root=None,
                dry_run=True,
                output=None,
            )
            handle_plan(args)
            self.assertFalse((root / "runs").exists())
            args.dry_run = False
            handle_plan(args)
            self.assertTrue(any((root / "runs").glob("**/submit_manifest.json")))
            ledgers = list((root / "runs").glob("**/ledger.json"))
            self.assertEqual(len(ledgers), 1)
            ledger = json.loads(ledgers[0].read_text())
            self.assertEqual(ledger["state"], "planned")

    def test_estimate_command_renders_resource_summary(self) -> None:
        from slurmforge.cli.estimate import handle_estimate

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_estimate(
                    Namespace(
                        config=str(cfg_path),
                        set=[],
                        project_root=None,
                        json=False,
                        output=None,
                    )
                )

            text = stdout.getvalue()
            self.assertIn("[ESTIMATE] project=demo experiment=stage_pipeline runs=1", text)
            self.assertIn("Stage train:", text)
            self.assertIn("peak_concurrent_gpus", text)

    def test_estimate_command_renders_heterogeneous_gpu_sizing(self) -> None:
        from slurmforge.cli.estimate import handle_estimate

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "hardware": {
                        "gpu_types": {
                            "a100_80gb": {
                                "memory_gb": 80,
                                "usable_memory_fraction": 0.90,
                                "max_gpus_per_node": 8,
                            }
                        }
                    },
                    "sizing": {"gpu": {"defaults": {"safety_factor": 1.0, "round_to": 1}}},
                    "runs": {
                        "type": "cases",
                        "cases": [
                            {"name": "small", "set": {"train.gpu_sizing.target_memory_gb": 80}},
                            {"name": "large", "set": {"train.gpu_sizing.target_memory_gb": 192}},
                        ],
                    },
                    "dispatch": {"max_available_gpus": 8, "overflow_policy": "serialize_groups"},
                },
            )
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["resources"]["gpu_type"] = "a100_80gb"
            payload["stages"]["train"]["resources"]["gpus_per_node"] = "auto"
            payload["stages"]["train"]["gpu_sizing"] = {
                "estimator": "heuristic",
                "target_memory_gb": 80,
                "min_gpus_per_job": 1,
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_estimate(
                    Namespace(
                        config=str(cfg_path),
                        set=[],
                        project_root=None,
                        json=False,
                        output=None,
                    )
                )

            text = stdout.getvalue()
            self.assertIn("sizing[1].resolved_gpus_per_node: 2", text)
            self.assertIn("sizing[2].resolved_gpus_per_node: 3", text)

    def test_resubmit_machine_dry_run_stdout_is_pure_json(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit
        from slurmforge.status import StageStatusRecord, commit_stage_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(train_batch.submission_root), 1, 0), 0)
            runs, bindings = upstream_bindings_from_train_batch(spec, Path(train_batch.submission_root))
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)
            eval_root = Path(eval_batch.submission_root)
            eval_instance = eval_batch.stage_instances[0]
            commit_stage_status(
                eval_root / eval_instance.run_dir_rel,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=eval_instance.stage_instance_id,
                    run_id=eval_instance.run_id,
                    stage_name=eval_instance.stage_name,
                    state="failed",
                ),
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_resubmit(
                    Namespace(
                        root=str(eval_root),
                        stage="eval",
                        query="state=failed",
                        run_id=[],
                        set=[],
                        dry_run="json",
                        emit_only=False,
                        output=None,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["command"], "resubmit")
            self.assertEqual(payload["state"], "valid")
            self.assertFalse(stdout.getvalue().startswith("[RESUBMIT]"))
            self.assertFalse((eval_root / "derived_batches").exists())

    def test_resubmit_empty_machine_dry_run_stdout_is_pure_json(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            train_root = Path(train_batch.submission_root)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_resubmit(
                    Namespace(
                        root=str(train_root),
                        stage="train",
                        query="state=failed",
                        run_id=[],
                        set=[],
                        dry_run="json",
                        emit_only=False,
                        output=None,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["command"], "resubmit")
            self.assertEqual(payload["plan_kind"], "empty_source_selection")
            self.assertEqual(payload["validation"]["selected_runs"], 0)
            self.assertFalse(stdout.getvalue().startswith("[RESUBMIT]"))

    def test_root_cli_only_exposes_stage_batch_commands(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        subparser_action = next(action for action in parser._actions if getattr(action, "choices", None))
        self.assertEqual(
            set(subparser_action.choices),
            {"validate", "estimate", "plan", "train", "eval", "run", "status", "resubmit"},
        )
        pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('sforge = "slurmforge.launcher:main"', pyproject)
        self.assertNotIn("sforge-stage-executor", pyproject)
        self.assertNotIn("sforge-pipeline-controller", pyproject)
