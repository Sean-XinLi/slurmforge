from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    build_shell_script,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
    write_stage_submit_files,
)
from tests.support.internal_records import materialize_stage_batch_for_test
import tempfile
from pathlib import Path


class StageSbatchEnvironmentTests(StageBatchSystemTestCase):
    def test_stage_sbatch_loads_environment_and_uses_python_module_executor(
        self,
    ) -> None:
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
                            "python": {
                                "bin": "/opt/env/bin/python",
                                "min_version": "3.10",
                            },
                            "module": "slurmforge.executor.stage",
                        },
                        "user": {
                            "default": {
                                "python": {"bin": "python3.11", "min_version": "3.10"}
                            }
                        },
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
                            "resources": {
                                "nodes": 1,
                                "gpus_per_node": 1,
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
                        },
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            self.assertEqual(
                batch.stage_instances[0].runtime_plan.executor.python.bin,
                "/opt/env/bin/python",
            )
            self.assertEqual(batch.stage_instances[0].environment_name, "train_env")
            self.assertEqual(batch.stage_instances[0].launcher_plan.type, "single")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
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
