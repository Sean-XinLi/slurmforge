from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import tempfile

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_train_eval_pipeline_for_test
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
)


class ResubmitRootTests(StageBatchSystemTestCase):
    def test_resubmit_repeated_emits_create_distinct_batch_roots(self) -> None:
        from slurmforge.cli.resubmit import handle_resubmit

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)

            for _ in range(2):
                handle_resubmit(
                    Namespace(
                        root=plan.root_dir,
                        stage="eval",
                        query="state=planned",
                        run_id=[],
                        set=[],
                        dry_run=False,
                        emit_only=True,
                    )
                )

            resubmit_roots = sorted(
                (Path(plan.root_dir) / "derived_batches").glob("eval_batch_*")
            )
            self.assertEqual(len(resubmit_roots), 2)
            self.assertNotEqual(resubmit_roots[0].name, resubmit_roots[1].name)

    def test_resubmit_root_reservation_is_new_only(self) -> None:
        from slurmforge.storage.derived_roots import reserve_derived_stage_batch_root

        with tempfile.TemporaryDirectory() as tmp:
            source_root = Path(tmp)

            first = reserve_derived_stage_batch_root(source_root, "eval_batch_contract")
            second = reserve_derived_stage_batch_root(
                source_root, "eval_batch_contract"
            )

            self.assertEqual(first.batch_id, "eval_batch_contract")
            self.assertEqual(second.batch_id, "eval_batch_contract_r0002")
            self.assertTrue(first.root.exists())
            self.assertTrue(second.root.exists())
            self.assertNotEqual(first.root, second.root)
