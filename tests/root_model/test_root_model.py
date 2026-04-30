from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    SchemaVersion,
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
    materialize_stage_batch_for_test,
)
import tempfile
from pathlib import Path


class RootModelTests(StageBatchSystemTestCase):
    def test_detects_stage_batch_and_pipeline_roots(self) -> None:
        from slurmforge.root_model.detection import detect_root
        from slurmforge.root_model.runs import (
            iter_all_stage_run_dirs,
            iter_runtime_stage_run_dirs,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)

            batch_descriptor = detect_root(Path(batch.submission_root))
            self.assertEqual(batch_descriptor.kind, "stage_batch")
            self.assertEqual(
                len(tuple(iter_runtime_stage_run_dirs(batch_descriptor.root))), 1
            )

            pipeline = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(pipeline, spec_snapshot=spec.raw)
            pipeline_descriptor = detect_root(Path(pipeline.root_dir))
            self.assertEqual(pipeline_descriptor.kind, "train_eval_pipeline")
            self.assertEqual(
                len(tuple(iter_runtime_stage_run_dirs(pipeline_descriptor.root))), 1
            )
            self.assertEqual(
                len(tuple(iter_all_stage_run_dirs(pipeline_descriptor.root))), 2
            )

    def test_invalid_root_is_user_facing_config_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.root_model.detection import detect_root

        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(ConfigContractError, "not a stage batch"),
        ):
            detect_root(Path(tmp))

    def test_refresh_status_snapshots_write_root_read_models(self) -> None:
        from slurmforge.root_model.snapshots import (
            refresh_stage_batch_status,
            refresh_train_eval_pipeline_status,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            batch_snapshot = refresh_stage_batch_status(Path(batch.submission_root))
            self.assertEqual(batch_snapshot.kind, "stage_batch")
            self.assertEqual(batch_snapshot.run_statuses[0].state, "planned")
            self.assertTrue((Path(batch.submission_root) / "run_status.json").exists())

            pipeline = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(pipeline, spec_snapshot=spec.raw)
            pipeline_snapshot = refresh_train_eval_pipeline_status(
                Path(pipeline.root_dir)
            )
            self.assertEqual(pipeline_snapshot.kind, "train_eval_pipeline")
            self.assertEqual(pipeline_snapshot.pipeline_status.state, "planned")
            self.assertTrue((Path(pipeline.root_dir) / "run_status.json").exists())
            self.assertTrue(
                (Path(pipeline.root_dir) / "train_eval_pipeline_status.json").exists()
            )

    def test_status_aggregation_has_one_canonical_state_order(self) -> None:
        from slurmforge.root_model.aggregation import (
            aggregate_run_status,
            aggregate_train_eval_pipeline_status,
        )
        from slurmforge.status.models import StageStatusRecord

        statuses = [
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id="run_1.train",
                run_id="run_1",
                stage_name="train",
                state="success",
            ),
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id="run_1.eval",
                run_id="run_1",
                stage_name="eval",
                state="failed",
            ),
        ]

        run_status = aggregate_run_status("run_1", statuses)
        pipeline_status = aggregate_train_eval_pipeline_status("pipeline_1", statuses)
        self.assertEqual(run_status.state, "failed")
        self.assertEqual(pipeline_status.state, "failed")
        self.assertEqual(pipeline_status.stage_counts["eval"]["failed"], 1)

    def test_notification_summary_uses_root_status_snapshot(self) -> None:
        from slurmforge.root_model.notifications import load_root_notification_snapshot
        from slurmforge.root_model.runs import iter_runtime_stage_run_dirs
        from slurmforge.status.machine import commit_stage_status
        from slurmforge.status.models import StageStatusRecord

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            run_dir = next(iter_runtime_stage_run_dirs(Path(batch.submission_root)))
            commit_stage_status(
                run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=batch.stage_instances[0].stage_instance_id,
                    run_id=batch.stage_instances[0].run_id,
                    stage_name="train",
                    state="failed",
                    failure_class="script_error",
                    reason="unit test failure",
                ),
            )

            snapshot = load_root_notification_snapshot(
                Path(batch.submission_root), event="stage_batch_finished"
            )
            self.assertEqual(snapshot.status.run_statuses[0].state, "failed")
            self.assertEqual(snapshot.summary_input.state, "failed")
            self.assertEqual(snapshot.summary_input.run_statuses[0].state, "failed")
            self.assertEqual(
                snapshot.summary_input.stage_statuses[0].failure_class, "script_error"
            )
