from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.std import Namespace, Path, tempfile, yaml


class SpecValidationTests(StageBatchSystemTestCase):
    def test_shared_types_have_single_public_import_path(self) -> None:
        import slurmforge.plans as plans
        import slurmforge.spec as spec

        for module in (plans, spec):
            exported = set(getattr(module, "__all__", ()))
            self.assertNotIn("InputSource", exported)
            self.assertNotIn("InputInjection", exported)
            self.assertNotIn("to_jsonable", exported)
            self.assertNotIn("stable_json", exported)

    def test_bad_output_contract_fails_validation_before_planning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "stages": {
                        "train": {
                            "kind": "train",
                            "entry": {
                                "type": "python_script",
                                "script": "train.py",
                                "workdir": str(root),
                            },
                            "outputs": {
                                "checkpoint": {
                                    "kind": "file",
                                    "required": True,
                                    "discover": {"globs": [], "select": "latest_step"},
                                }
                            },
                        },
                        "eval": {
                            "kind": "eval",
                            "entry": {
                                "type": "python_script",
                                "script": "eval.py",
                                "workdir": str(root),
                            },
                            "inputs": {
                                "checkpoint": {
                                    "source": {"kind": "upstream_output", "stage": "train", "output": "checkpoint"},
                                    "expects": "path",
                                    "required": True,
                                }
                            },
                        },
                    }
                },
            )

            with self.assertRaisesRegex(Exception, "outputs.checkpoint.discover.globs"):
                load_experiment_spec(cfg_path)

    def test_legacy_top_level_schema_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\nexperiment_name: old\nrun: {}\n", encoding="utf-8")
            with self.assertRaisesRegex(Exception, "stages"):
                load_experiment_spec(cfg_path)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["common"] = {"env": {"extra_env": {"OLD": "1"}}}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "Unsupported top-level keys: common"):
                load_experiment_spec(cfg_path)

    def test_top_level_matrix_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload.pop("runs")
            payload["matrix"] = {"axes": {"train.entry.args.lr": [0.001, 0.002]}}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "Top-level `matrix` is not supported"):
                load_experiment_spec(cfg_path)

    def test_case_runs_require_valid_unique_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runs"] = {
                "type": "cases",
                "cases": [
                    {"name": "bad/name", "set": {"train.entry.args.lr": 0.001}},
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "letters, numbers, underscores, dots, and dashes"):
                load_experiment_spec(cfg_path)

            payload["runs"] = {
                "type": "cases",
                "cases": [
                    {"name": "dup", "set": {"train.entry.args.lr": 0.001}},
                    {"name": "dup", "set": {"train.entry.args.lr": 0.002}},
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "must be unique: dup"):
                load_experiment_spec(cfg_path)

    def test_case_runs_validate_set_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runs"] = {
                "type": "cases",
                "cases": [
                    {"name": "bad_path", "set": {"train.resources.missing": 1}},
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "runs.cases.bad_path.set.train.resources.missing"):
                load_experiment_spec(cfg_path)

    def test_email_notifications_require_recipients_and_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["notifications"] = {
                "email": {
                    "enabled": True,
                    "to": ["you@example.com"],
                    "on": ["batch_finished", "train_eval_pipeline_finished"],
                    "mode": "summary",
                }
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            spec = load_experiment_spec(cfg_path)
            self.assertTrue(spec.notifications.email.enabled)
            self.assertEqual(spec.notifications.email.to, ("you@example.com",))
            self.assertEqual(spec.notifications.email.events, ("batch_finished", "train_eval_pipeline_finished"))

            payload["notifications"]["email"]["to"] = []
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "notifications.email.to"):
                load_experiment_spec(cfg_path)

            payload["notifications"]["email"]["to"] = ["you@example.com"]
            payload["notifications"]["email"]["on"] = []
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "notifications.email.on"):
                load_experiment_spec(cfg_path)

    def test_auto_gpu_sizing_contract_is_strict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["hardware"] = {
                "gpu_types": {
                    "a100_80gb": {
                        "memory_gb": 80,
                        "usable_memory_fraction": 0.90,
                        "max_gpus_per_node": 8,
                    }
                }
            }
            payload["sizing"] = {"gpu": {"defaults": {"safety_factor": 1.15, "round_to": 1}}}
            payload["dispatch"]["max_available_gpus"] = 8
            payload["stages"]["train"]["resources"]["gpu_type"] = "a100_80gb"
            payload["stages"]["train"]["resources"]["gpus_per_node"] = "auto"
            payload["stages"]["train"]["gpu_sizing"] = {
                "estimator": "heuristic",
                "target_memory_gb": 192,
                "min_gpus_per_job": 4,
                "max_gpus_per_job": 4,
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            spec = load_experiment_spec(cfg_path)
            self.assertEqual(spec.stages["train"].resources.gpus_per_node, "auto")

            payload["stages"]["train"]["resources"]["gpus_per_node"] = 4
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "gpu_sizing.*only allowed"):
                load_experiment_spec(cfg_path)

            payload["stages"]["train"]["resources"]["gpus_per_node"] = "auto"
            payload["stages"]["train"]["resources"]["gpu_type"] = "missing"
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "unknown gpu type `missing`"):
                load_experiment_spec(cfg_path)

    def test_orchestration_controller_uses_nested_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            spec = load_experiment_spec(cfg_path)

            self.assertEqual(spec.orchestration.controller.partition, "cpu")
            self.assertEqual(spec.orchestration.controller.cpus, 1)
            self.assertEqual(spec.orchestration.controller.mem, "2G")
            self.assertEqual(spec.orchestration.controller.time_limit, "01:00:00")

            payload = yaml.safe_load(cfg_path.read_text())
            payload["orchestration"] = {"controller_partition": "cpu"}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "Unsupported keys under `orchestration`: controller_partition"):
                load_experiment_spec(cfg_path)

    def test_runtime_bootstrap_is_replaced_by_environments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runtime"]["executor"]["bootstrap"] = {"steps": []}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "Unsupported keys under `runtime.executor`: bootstrap"):
                load_experiment_spec(cfg_path)

    def test_stage_environment_must_reference_declared_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["environment"] = "missing_env"
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "unknown environment `missing_env`"):
                load_experiment_spec(cfg_path)

    def test_files_output_discovery_does_not_accept_select(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["outputs"]["train_logs"] = {
                "kind": "files",
                "discover": {"globs": ["logs/**/*.log"], "select": "last"},
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "select.*file outputs"):
                load_experiment_spec(cfg_path)

    def test_eval_input_source_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"]["checkpoint"].pop("source")
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "source"):
                load_experiment_spec(cfg_path)

    def test_upstream_input_source_must_reference_declared_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"]["checkpoint"]["source"]["output"] = "missing_checkpoint"
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "missing output `train.missing_checkpoint`"):
                load_experiment_spec(cfg_path)

    def test_input_expects_must_match_upstream_output_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["outputs"]["score"] = {
                "kind": "metric",
                "file": "metrics/train.json",
                "json_path": "$.score",
                "required": True,
            }
            payload["stages"]["eval"]["inputs"]["checkpoint"] = {
                "source": {"kind": "upstream_output", "stage": "train", "output": "score"},
                "expects": "path",
                "required": True,
                "inject": {"flag": "score", "env": "SFORGE_INPUT_SCORE", "mode": "path"},
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "expects=path.*kind=metric"):
                load_experiment_spec(cfg_path)

    def test_input_inject_mode_must_match_expects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"]["checkpoint"]["inject"]["mode"] = "value"
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "inject.mode.*expects=path"):
                load_experiment_spec(cfg_path)

    def test_eval_plan_without_source_is_logical_only(self) -> None:
        from slurmforge.cli.plan import handle_plan

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            args = Namespace(
                plan_command="eval",
                config=str(cfg_path),
                set=[],
                project_root=None,
                checkpoint=None,
                from_train_batch=None,
                from_run=None,
                dry_run=True,
                output=None,
            )
            handle_plan(args)
            self.assertFalse(any((root / "runs").glob("**/submit_manifest.json")))
            args.dry_run = False
            with self.assertRaisesRegex(Exception, "logical preview"):
                handle_plan(args)
            self.assertFalse(any((root / "runs").glob("**/submit_manifest.json")))

    def test_planner_rejects_empty_run_selection_and_missing_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            with self.assertRaisesRegex(Exception, "at least one run"):
                compile_stage_batch_for_kind(spec, kind="train", runs=())
            run = compile_stage_batch_for_kind(spec, kind="train").stage_instances[0]
            from slurmforge.plans import RunDefinition

            selected_run = RunDefinition(
                run_id=run.run_id,
                run_index=run.run_index,
                run_overrides=dict(run.run_overrides),
                spec_snapshot_digest=run.spec_snapshot_digest,
            )
            with self.assertRaisesRegex(Exception, "missing"):
                compile_stage_batch_for_kind(
                    spec,
                    kind="eval",
                    runs=(selected_run,),
                    input_bindings_by_run={},
                )
