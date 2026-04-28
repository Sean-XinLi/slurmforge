from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    build_shell_script,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
import tempfile
import yaml
from pathlib import Path


class StageSbatchTorchrunTests(StageBatchSystemTestCase):
    def test_torchrun_launcher_auto_resolves_from_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "dispatch": {
                        "max_available_gpus": 8,
                        "overflow_policy": "serialize_groups",
                    },
                    "stages": {
                        "train": {
                            "kind": "train",
                            "entry": {
                                "type": "python_script",
                                "script": "train.py",
                                "workdir": str(root),
                            },
                            "launcher": {
                                "type": "torchrun",
                                "nnodes": "auto",
                                "nproc_per_node": "auto",
                            },
                            "resources": {
                                "nodes": 2,
                                "gpus_per_node": 4,
                                "cpus_per_task": 1,
                            },
                            "outputs": {
                                "checkpoint": {
                                    "kind": "file",
                                    "required": True,
                                    "discover": {
                                        "globs": ["checkpoints/**/*.pt"],
                                        "select": "latest_step",
                                    },
                                }
                            },
                        },
                        "eval": {
                            "kind": "eval",
                            "enabled": False,
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
                    },
                },
            )
            batch = compile_stage_batch_for_kind(
                load_experiment_spec(cfg_path), kind="train"
            )
            launcher = batch.stage_instances[0].launcher_plan
            self.assertEqual(launcher.type, "torchrun")
            self.assertEqual(launcher.mode, "multi_node")
            self.assertEqual(launcher.nnodes, 2)
            self.assertEqual(launcher.nproc_per_node, 4)
            command = build_shell_script(batch.stage_instances[0], ())
            self.assertIn("srun --nodes 2 --ntasks 2 --ntasks-per-node 1", command)
            self.assertIn("torch.distributed.run", command)
            self.assertIn("--node-rank", command)
            self.assertIn("--rdzv-endpoint", command)

    def test_torchrun_validation_rejects_launcher_resource_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["enabled"] = False
            payload["stages"]["train"]["launcher"] = {
                "type": "torchrun",
                "mode": "single_node",
            }
            payload["stages"]["train"]["resources"]["nodes"] = 2
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "resources.nodes == 1"):
                load_experiment_spec(cfg_path)

            payload["stages"]["train"]["launcher"] = {
                "type": "torchrun",
                "mode": "multi_node",
                "nnodes": 3,
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "must equal resources.nodes"):
                load_experiment_spec(cfg_path)

            payload["stages"]["train"]["launcher"] = {
                "type": "torchrun",
                "mode": "multi_node",
                "nnodes": 2,
                "nproc_per_node": 8,
            }
            payload["stages"]["train"]["resources"]["gpus_per_node"] = 4
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                Exception, "cannot exceed resources.gpus_per_node"
            ):
                load_experiment_spec(cfg_path)

            payload["stages"]["train"]["launcher"] = {
                "type": "torchrun",
                "mode": "multi_node",
                "nnodes": 2,
                "nproc_per_node": 4,
                "rendezvous": {"port": 70000},
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "between 1 and 65535"):
                load_experiment_spec(cfg_path)
