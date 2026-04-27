from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    build_shell_script,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    load_stage_submit_manifest,
    write_demo_project,
    write_stage_batch_layout,
    write_stage_submit_files,
)
from tests.support.std import Path, tempfile, yaml


class StageSbatchTests(StageBatchSystemTestCase):
    def test_stage_sbatch_loads_environment_and_uses_python_module_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "environments": {
                        "train_env": {
                            "modules": ["cuda/12.1"],
                            "source": [
                                {
                                    "path": "/shared/miniconda/bin/activate",
                                    "args": ["myenv"],
                                }
                            ],
                            "env": {"HF_HOME": "/shared/hf"},
                        }
                    },
                    "runtime": {
                        "executor": {
                            "python": {"bin": "/opt/env/bin/python", "min_version": "3.10"},
                            "module": "slurmforge.executor.stage",
                        },
                        "user": {"default": {"python": {"bin": "python3.11", "min_version": "3.10"}}},
                    },
                    "stages": {
                        "train": {
                            "kind": "train",
                            "environment": "train_env",
                            "entry": {
                                "type": "python_script",
                                "script": "train.py",
                                "workdir": str(root),
                            },
                            "before": [
                                {"name": "check-gpu", "run": "nvidia-smi"},
                            ],
                            "resources": {"nodes": 1, "gpus_per_node": 1, "cpus_per_task": 1},
                            "outputs": {
                                "checkpoint": {
                                    "kind": "file",
                                    "required": True,
                                    "discover": {"globs": ["checkpoints/**/*.pt"], "select": "latest_step"},
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
                        },
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            self.assertEqual(batch.stage_instances[0].runtime_plan.executor.python.bin, "/opt/env/bin/python")
            self.assertEqual(batch.stage_instances[0].environment_name, "train_env")
            self.assertEqual(batch.stage_instances[0].launcher_plan.type, "single")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            sbatch_path = write_stage_submit_files(batch)[0]
            sbatch = sbatch_path.read_text()
            self.assertIn("module load cuda/12.1", sbatch)
            self.assertIn("source /shared/miniconda/bin/activate myenv", sbatch)
            self.assertIn("export HF_HOME=/shared/hf", sbatch)
            self.assertIn("/opt/env/bin/python -m slurmforge.executor.stage", sbatch)
            self.assertNotIn("sforge-stage-executor", sbatch)
            stage_shell = build_shell_script(batch.stage_instances[0], ())
            self.assertNotIn("module load cuda/12.1", stage_shell)
            self.assertNotIn("source /shared/miniconda/bin/activate myenv", stage_shell)
            self.assertIn("[BEFORE] check-gpu", stage_shell)
            self.assertIn("nvidia-smi", stage_shell)

    def test_stage_submit_files_include_batch_notification_finalizer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "notifications": {
                        "email": {
                            "enabled": True,
                            "to": ["you@example.com"],
                            "on": ["batch_finished"],
                            "mode": "summary",
                        }
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            write_stage_submit_files(batch)
            manifest = load_stage_submit_manifest(Path(batch.submission_root))
            notifications = manifest["notifications"]
            self.assertEqual(notifications[0]["event"], "batch_finished")
            notify_path = Path(notifications[0]["sbatch_path"])
            self.assertTrue(notify_path.exists())
            notify_text = notify_path.read_text()
            self.assertIn("slurmforge.submission.finalizer", notify_text)
            submit_text = Path(manifest["submit_script"]).read_text()
            self.assertNotIn("notify_batch_finished", submit_text)

    def test_torchrun_launcher_auto_resolves_from_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "dispatch": {"max_available_gpus": 8, "overflow_policy": "serialize_groups"},
                    "stages": {
                        "train": {
                            "kind": "train",
                            "entry": {
                                "type": "python_script",
                                "script": "train.py",
                                "workdir": str(root),
                            },
                            "launcher": {"type": "torchrun", "nnodes": "auto", "nproc_per_node": "auto"},
                            "resources": {"nodes": 2, "gpus_per_node": 4, "cpus_per_task": 1},
                            "outputs": {
                                "checkpoint": {
                                    "kind": "file",
                                    "required": True,
                                    "discover": {"globs": ["checkpoints/**/*.pt"], "select": "latest_step"},
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
                                    "source": {"kind": "upstream_output", "stage": "train", "output": "checkpoint"},
                                    "expects": "path",
                                    "required": True,
                                }
                            },
                        },
                    }
                },
            )
            batch = compile_stage_batch_for_kind(load_experiment_spec(cfg_path), kind="train")
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
            payload["stages"]["train"]["launcher"] = {"type": "torchrun", "mode": "single_node"}
            payload["stages"]["train"]["resources"]["nodes"] = 2
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "resources.nodes == 1"):
                load_experiment_spec(cfg_path)

            payload["stages"]["train"]["launcher"] = {"type": "torchrun", "mode": "multi_node", "nnodes": 3}
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
            with self.assertRaisesRegex(Exception, "cannot exceed resources.gpus_per_node"):
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
