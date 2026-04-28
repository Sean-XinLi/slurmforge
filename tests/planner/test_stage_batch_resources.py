from __future__ import annotations

from pathlib import Path
import tempfile

import yaml

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)


class StageBatchResourceTests(StageBatchSystemTestCase):
    def test_compile_stage_batch_materializes_auto_gpu_sizing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "hardware": {
                            "gpu_types": {
                                "a100_80gb": {
                                    "memory_gb": 80,
                                    "usable_memory_fraction": 0.90,
                                    "max_gpus_per_node": 8,
                                    "slurm": {"constraint": "a100"},
                                }
                            }
                        },
                        "sizing": {"gpu": {"defaults": {"safety_factor": 1.15}}},
                        "dispatch": {
                            "max_available_gpus": 8,
                            "overflow_policy": "serialize_groups",
                        },
                    },
                )
            )
            payload = yaml.safe_load(spec.config_path.read_text())
            payload["stages"]["train"]["resources"]["gpu_type"] = "a100_80gb"
            payload["stages"]["train"]["resources"]["gpus_per_node"] = "auto"
            payload["stages"]["train"]["resources"].pop("constraint", None)
            payload["stages"]["train"]["gpu_sizing"] = {
                "estimator": "heuristic",
                "target_memory_gb": 192,
                "min_gpus_per_job": 4,
                "max_gpus_per_job": 4,
            }
            spec.config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            spec = load_experiment_spec(spec.config_path)

            batch = compile_stage_batch_for_kind(spec, kind="train")
            instance = batch.stage_instances[0]

            self.assertEqual(instance.resources.gpus_per_node, 4)
            self.assertEqual(instance.resources.constraint, "a100")
            self.assertEqual(instance.resource_sizing.mode, "auto")
            self.assertEqual(instance.resource_sizing.resolved_total_gpus, 4)
            self.assertEqual(batch.group_plans[0].gpus_per_task, 4)
