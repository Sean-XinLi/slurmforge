from __future__ import annotations

from tests.support import *  # noqa: F401,F403


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
                matrix_assignments=dict(run.matrix_assignments),
                spec_snapshot_digest=run.spec_snapshot_digest,
            )
            with self.assertRaisesRegex(Exception, "missing"):
                compile_stage_batch_for_kind(
                    spec,
                    kind="eval",
                    runs=(selected_run,),
                    input_bindings_by_run={},
                )
