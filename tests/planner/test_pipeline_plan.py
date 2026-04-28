from __future__ import annotations

from pathlib import Path
import tempfile

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    write_demo_project,
)


class PipelinePlanTests(StageBatchSystemTestCase):
    def test_compile_train_eval_pipeline_plan_keeps_train_and_eval_as_separate_batches(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))

            plan = compile_train_eval_pipeline_plan(spec)

            self.assertEqual(plan.pipeline_kind, "train_eval_pipeline")
            self.assertTrue(plan.pipeline_id.startswith("train_eval_pipeline_"))
            self.assertEqual(plan.stage_order, ("train", "eval"))
            self.assertEqual(set(plan.stage_batches), {"train", "eval"})
            self.assertEqual(plan.stage_batches["train"].stage_name, "train")
            self.assertEqual(plan.stage_batches["eval"].stage_name, "eval")
            self.assertNotEqual(
                plan.stage_batches["train"].submission_root,
                plan.stage_batches["eval"].submission_root,
            )
