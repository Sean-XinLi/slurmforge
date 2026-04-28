from __future__ import annotations

from pathlib import Path
import tempfile

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)


class StageBatchRunTests(StageBatchSystemTestCase):
    def test_compile_stage_batch_groups_grid_runs_by_resource_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "runs": {
                            "type": "grid",
                            "axes": {
                                "train.entry.args.lr": [0.001, 0.002],
                                "train.resources.constraint": ["a", "b"],
                            },
                        },
                    },
                )
            )

            batch = compile_stage_batch_for_kind(spec, kind="train")

            self.assertEqual(batch.stage_name, "train")
            self.assertEqual(len(batch.selected_runs), 4)
            self.assertEqual(sum(group.array_size for group in batch.group_plans), 4)
            self.assertEqual(
                {group.resources.constraint for group in batch.group_plans}, {"a", "b"}
            )

    def test_compile_stage_batch_accepts_case_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "runs": {
                            "type": "cases",
                            "cases": [
                                {
                                    "name": "small_gpu",
                                    "set": {
                                        "train.entry.args.lr": 0.001,
                                        "train.resources.constraint": "a",
                                    },
                                },
                                {
                                    "name": "large_gpu",
                                    "set": {
                                        "train.entry.args.lr": 0.002,
                                        "train.resources.constraint": "b",
                                    },
                                },
                            ],
                        },
                    },
                )
            )

            batch = compile_stage_batch_for_kind(spec, kind="train")

            self.assertEqual(batch.selected_runs, ("small_gpu", "large_gpu"))
            self.assertEqual(
                {group.resources.constraint for group in batch.group_plans}, {"a", "b"}
            )

    def test_compile_stage_batch_accepts_matrix_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "runs": {
                            "type": "matrix",
                            "cases": [
                                {
                                    "name": "small_model",
                                    "set": {
                                        "train.entry.args.model": "small",
                                        "train.resources.constraint": "small",
                                    },
                                    "axes": {
                                        "train.entry.args.lr": [0.001, 0.002],
                                        "train.entry.args.seed": [1, 2],
                                    },
                                },
                                {
                                    "name": "large_model",
                                    "set": {
                                        "train.entry.args.model": "large",
                                        "train.resources.constraint": "large",
                                    },
                                    "axes": {
                                        "train.entry.args.lr": [0.0001],
                                        "train.entry.args.seed": [1, 2],
                                    },
                                },
                            ],
                        },
                    },
                )
            )

            batch = compile_stage_batch_for_kind(spec, kind="train")

            self.assertEqual(len(batch.selected_runs), 6)
            self.assertTrue(batch.selected_runs[0].startswith("small_model.grid_0001_"))
            self.assertTrue(batch.selected_runs[-1].startswith("large_model.grid_0002_"))
            self.assertEqual(
                {group.resources.constraint for group in batch.group_plans},
                {"small", "large"},
            )
            first = batch.stage_instances[0]
            self.assertEqual(first.run_overrides["train.entry.args.model"], "small")
            self.assertIn("train.entry.args.lr", first.run_overrides)
