from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from tests.control.pipeline_overlays import grid_runs
from tests.control.pipeline_overlays import with_current_python
from tests.helpers.overlays import apply_overlay
from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_train_eval_pipeline_for_test
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    write_demo_project,
)


class PipelineBudgetTests(StageBatchSystemTestCase):
    def test_active_gpu_budget_uses_array_throttle_not_submitted_task_count(
        self,
    ) -> None:
        from slurmforge.control.dispatch_budget import active_budgeted_gpus
        from slurmforge.control.state import load_workflow_state
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra=with_current_python(
                        apply_overlay(
                            grid_runs(4),
                            {"dispatch": {"max_available_gpus": 2}},
                        )
                    ),
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            submit_initial_pipeline(plan, client=FakeSlurmClient())

            state = load_workflow_state(Path(plan.root_dir), plan)
            shutil.rmtree(Path(state.submissions["train_initial"].root))
            self.assertEqual(active_budgeted_gpus(state), 2)
