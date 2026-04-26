from __future__ import annotations

from tests.support import *  # noqa: F401,F403


class OrchestrationTests(StageBatchSystemTestCase):
    def test_orchestration_facade_exposes_only_high_level_verbs(self) -> None:
        import slurmforge.orchestration as orchestration

        self.assertTrue(hasattr(orchestration, "build_prior_source_stage_batch"))
        self.assertTrue(hasattr(orchestration, "emit_sourced_stage_batch"))
        self.assertTrue(hasattr(orchestration, "render_status"))
        self.assertFalse(hasattr(orchestration, "compile_stage_batch_from_prior_source"))
        self.assertFalse(hasattr(orchestration, "materialize_sourced_stage_batch_plan"))
        self.assertFalse(hasattr(orchestration, "prepare_stage_submission"))
        self.assertFalse(hasattr(orchestration, "read_controller_status"))

    def test_emit_sourced_stage_batch_materializes_and_prepares_submit_files(self) -> None:
        from slurmforge.orchestration import build_prior_source_stage_batch, emit_sourced_stage_batch

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            pipeline = compile_pipeline_plan(spec)
            write_pipeline_layout(pipeline, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(pipeline.stage_batches["train"].submission_root), 1, 0), 0)
            plan = build_prior_source_stage_batch(
                source_root=Path(pipeline.root_dir),
                stage_name="eval",
                query="state=planned",
            )
            assert plan is not None

            concrete, group_job_ids = emit_sourced_stage_batch(plan, submit=False)

            batch_root = Path(concrete.batch.submission_root)
            self.assertIsNone(group_job_ids)
            self.assertTrue((batch_root / "source_plan.json").exists())
            self.assertTrue((batch_root / "submit" / "submit_manifest.json").exists())
            self.assertTrue(next(batch_root.glob("runs/*/input_bindings.json")).exists())

    def test_render_status_reports_stage_counts(self) -> None:
        from slurmforge.orchestration import render_status

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)

            rendered = io.StringIO()
            with redirect_stdout(rendered):
                render_status(root=Path(batch.submission_root))

            output = rendered.getvalue()
            self.assertIn("total_stages=1", output)
            self.assertIn("stage=train", output)
