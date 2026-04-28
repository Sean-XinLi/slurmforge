from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
import tempfile
from argparse import Namespace
from pathlib import Path


class PlanningValidationTests(StageBatchSystemTestCase):
    def test_eval_plan_without_source_is_logical_only(self) -> None:
        from slurmforge.cli.plan import handle_plan

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            args = Namespace(
                plan_command="eval",
                config=str(cfg_path),
                set=[],
                project_root=None,
                checkpoint=None,
                from_train_batch=None,
                from_run=None,
                dry_run=True,
                output=None,
            )
            handle_plan(args)
            self.assertFalse(any((root / "runs").glob("**/submit_manifest.json")))
            args.dry_run = False
            with self.assertRaisesRegex(Exception, "logical preview"):
                handle_plan(args)
            self.assertFalse(any((root / "runs").glob("**/submit_manifest.json")))

    def test_planner_rejects_empty_run_selection_and_missing_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            with self.assertRaisesRegex(Exception, "at least one run"):
                compile_stage_batch_for_kind(spec, kind="train", runs=())
            run = compile_stage_batch_for_kind(spec, kind="train").stage_instances[0]
            from slurmforge.contracts import RunDefinition

            selected_run = RunDefinition(
                run_id=run.run_id,
                run_index=run.run_index,
                run_overrides=dict(run.run_overrides),
                spec_snapshot_digest=run.spec_snapshot_digest,
            )
            with self.assertRaisesRegex(Exception, "missing"):
                compile_stage_batch_for_kind(
                    spec,
                    kind="eval",
                    runs=(selected_run,),
                    input_bindings_by_run={},
                )
