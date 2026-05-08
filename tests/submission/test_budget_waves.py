from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    load_submit_manifest,
    write_demo_project,
    write_stage_submit_files,
)
from tests.support.internal_records import (
    materialize_stage_batch_for_test,
)
import tempfile
from pathlib import Path


class BudgetWaveTests(StageBatchSystemTestCase):
    def test_gpu_budget_uses_global_waves_without_exceeding_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.resources.gpus_per_node": [1, 2]},
                    },
                    "dispatch": {
                        "max_available_gpus": 2,
                        "overflow_policy": "serialize_groups",
                    },
                },
            )
            batch = compile_stage_batch_for_kind(
                load_experiment_spec(cfg_path), kind="train"
            )
            self.assertEqual(batch.budget_plan.policy_applied, "global_waves")
            self.assertEqual(batch.budget_plan.dependencies[0].type, "afterany")
            self.assertLessEqual(
                max(wave.total_wave_gpus for wave in batch.budget_plan.waves), 2
            )
            materialize_stage_batch_for_test(
                batch, spec_snapshot=load_experiment_spec(cfg_path).raw
            )
            write_stage_submit_files(batch)
            manifest = load_submit_manifest(Path(batch.submission_root))
            submit_text = Path(manifest.submit_script).read_text()
            self.assertIn("--dependency=afterany", submit_text)

    def test_gpu_budget_does_not_double_count_parallel_group_throttles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.resources.constraint": ["a", "b"]},
                    },
                    "dispatch": {
                        "max_available_gpus": 2,
                        "overflow_policy": "serialize_groups",
                    },
                },
            )
            batch = compile_stage_batch_for_kind(
                load_experiment_spec(cfg_path), kind="train"
            )
            self.assertEqual(len(batch.budget_plan.waves), 1)
            wave = batch.budget_plan.waves[0]
            self.assertEqual(wave.total_wave_gpus, 2)
            self.assertEqual([item.array_throttle for item in wave.groups], [1, 1])
