from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    load_experiment_spec,
    write_demo_project,
)
import tempfile
import yaml
from pathlib import Path


class SchemaContractTests(StageBatchSystemTestCase):
    def test_generated_starter_config_example_is_valid(self) -> None:
        from slurmforge.spec.parse_sections import parse_experiment_spec
        from slurmforge.spec.validation import validate_experiment_spec
        from slurmforge.starter.config_examples import render_starter_example

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = root / "starter.yaml"
            raw = yaml.safe_load(render_starter_example(root))

            spec = parse_experiment_spec(
                raw,
                config_path=cfg_path,
                project_root=root,
            )
            validate_experiment_spec(spec, check_paths=False)

    def test_generated_advanced_config_example_is_valid(self) -> None:
        from slurmforge.spec.parse_sections import parse_experiment_spec
        from slurmforge.spec.validation import validate_experiment_spec
        from slurmforge.starter.config_examples import render_advanced_example

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = root / "advanced.yaml"
            raw = yaml.safe_load(render_advanced_example())

            spec = parse_experiment_spec(
                raw,
                config_path=cfg_path,
                project_root=root,
            )
            validate_experiment_spec(spec, check_paths=False)

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
                                    "source": {
                                        "kind": "upstream_output",
                                        "stage": "train",
                                        "output": "checkpoint",
                                    },
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
            self.assertEqual(
                spec.notifications.email.events,
                ("batch_finished", "train_eval_pipeline_finished"),
            )

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
            payload["sizing"] = {
                "gpu": {"defaults": {"safety_factor": 1.15, "round_to": 1}}
            }
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
            with self.assertRaisesRegex(
                Exception,
                "Unsupported keys under `orchestration`: controller_partition",
            ):
                load_experiment_spec(cfg_path)

    def test_runtime_bootstrap_is_replaced_by_environments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runtime"]["executor"]["bootstrap"] = {"steps": []}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                Exception, "Unsupported keys under `runtime.executor`: bootstrap"
            ):
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

    def test_remaining_config_sections_reject_unknown_keys(self) -> None:
        cases = (
            (
                "artifact_store",
                lambda payload: payload["artifact_store"].update({"unknown": True}),
                "Unsupported keys under `artifact_store`: unknown",
            ),
            (
                "dispatch",
                lambda payload: payload["dispatch"].update({"unknown": True}),
                "Unsupported keys under `dispatch`: unknown",
            ),
            (
                "entry",
                lambda payload: payload["stages"]["train"]["entry"].update(
                    {"unknown": True}
                ),
                "Unsupported keys under `stages.train.entry`: unknown",
            ),
            (
                "input",
                lambda payload: payload["stages"]["eval"]["inputs"][
                    "checkpoint"
                ].update({"unknown": True}),
                "Unsupported keys under `stages.eval.inputs.checkpoint`: unknown",
            ),
            (
                "input inject",
                lambda payload: payload["stages"]["eval"]["inputs"]["checkpoint"][
                    "inject"
                ].update({"unknown": True}),
                "Unsupported keys under `stages.eval.inputs.checkpoint.inject`: unknown",
            ),
            (
                "input source",
                lambda payload: payload["stages"]["eval"]["inputs"]["checkpoint"][
                    "source"
                ].update({"unknown": True}),
                "Unsupported keys under `stages.eval.inputs.checkpoint.source`: unknown",
            ),
            (
                "output",
                lambda payload: payload["stages"]["train"]["outputs"][
                    "checkpoint"
                ].update({"unknown": True}),
                "Unsupported keys under `stages.train.outputs.checkpoint`: unknown",
            ),
            (
                "output discover",
                lambda payload: payload["stages"]["train"]["outputs"]["checkpoint"][
                    "discover"
                ].update({"unknown": True}),
                "Unsupported keys under `stages.train.outputs.checkpoint.discover`: unknown",
            ),
        )
        for name, mutate, message in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = write_demo_project(root)
                payload = yaml.safe_load(cfg_path.read_text())
                mutate(payload)
                cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

                with self.assertRaisesRegex(Exception, message):
                    load_experiment_spec(cfg_path)

    def test_removed_launcher_aliases_are_rejected(self) -> None:
        cases = (
            ("nodes", 2),
            ("processes_per_node", 4),
            ("master_port", 29501),
        )
        for key, value in cases:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = write_demo_project(root)
                payload = yaml.safe_load(cfg_path.read_text())
                payload["stages"]["train"]["launcher"] = {
                    "type": "torchrun",
                    key: value,
                }
                cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

                with self.assertRaisesRegex(
                    Exception,
                    f"Unsupported keys under `stages.train.launcher`: {key}",
                ):
                    load_experiment_spec(cfg_path)
