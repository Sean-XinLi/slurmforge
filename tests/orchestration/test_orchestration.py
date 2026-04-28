from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import (
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
import tempfile
from pathlib import Path


class OrchestrationTests(StageBatchSystemTestCase):
    def test_orchestration_facade_exposes_only_high_level_verbs(self) -> None:
        import slurmforge.orchestration as orchestration

        self.assertEqual(orchestration.__all__, [])
        self.assertFalse(hasattr(orchestration, "build_prior_source_stage_batch"))
        self.assertFalse(hasattr(orchestration, "emit_sourced_stage_batch"))
        self.assertFalse(hasattr(orchestration, "render_status_lines"))

    def test_emit_sourced_stage_batch_materializes_and_prepares_submit_files(
        self,
    ) -> None:
        from slurmforge.orchestration.launch import emit_sourced_stage_batch
        from slurmforge.orchestration.stage_build import build_prior_source_stage_batch

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            pipeline = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(pipeline, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(
                    Path(pipeline.stage_batches["train"].submission_root), 1, 0
                ),
                0,
            )
            plan = build_prior_source_stage_batch(
                source_root=Path(pipeline.root_dir),
                stage_name="eval",
                query="state=planned",
            )
            assert plan is not None

            result = emit_sourced_stage_batch(plan, submit=False)
            concrete = result.plan

            batch_root = Path(concrete.batch.submission_root)
            self.assertFalse(result.submitted)
            self.assertTrue((batch_root / "source_plan.json").exists())
            self.assertTrue((batch_root / "submit" / "submit_manifest.json").exists())
            self.assertTrue(
                next(batch_root.glob("runs/*/input_bindings.json")).exists()
            )

    def test_render_status_reports_stage_counts(self) -> None:
        from slurmforge.orchestration.status_view import render_status_lines

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)

            output = "\n".join(render_status_lines(root=Path(batch.submission_root)))
            self.assertIn("total_stages=1", output)
            self.assertIn("stage=train", output)
