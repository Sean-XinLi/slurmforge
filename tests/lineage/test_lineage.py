from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
import tempfile
from pathlib import Path


class LineageTests(StageBatchSystemTestCase):
    def test_stage_batch_lineage_records_bound_inputs_and_source_roots(self) -> None:
        from slurmforge.lineage.query import (
            find_bound_input,
            iter_lineage_source_roots,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            runs, bindings = upstream_bindings_from_train_batch(
                spec, Path(train_batch.submission_root)
            )
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            write_stage_batch_layout(eval_batch, spec_snapshot=spec.raw)
            eval_root = Path(eval_batch.submission_root)

            roots = tuple(iter_lineage_source_roots(eval_root))
            record = find_bound_input(
                eval_root, run_id=runs[0].run_id, input_name="checkpoint"
            )

            self.assertIn(Path(train_batch.submission_root).resolve(), roots)
            assert record is not None
            self.assertEqual(record["stage_name"], "eval")
            self.assertEqual(
                record["resolution"]["producer_root"],
                str(Path(train_batch.submission_root).resolve()),
            )

    def test_pipeline_lineage_lists_stage_batch_roots(self) -> None:
        from slurmforge.lineage.query import iter_lineage_source_roots

        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            roots = set(iter_lineage_source_roots(Path(plan.root_dir)))

            self.assertEqual(
                roots,
                {
                    Path(batch.submission_root).resolve()
                    for batch in plan.stage_batches.values()
                },
            )
