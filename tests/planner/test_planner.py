from __future__ import annotations

from tests.support import *  # noqa: F401,F403


class PlannerTests(StageBatchSystemTestCase):
    def test_compile_stage_batch_groups_matrix_runs_by_resource_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "matrix": {
                            "axes": {
                                "train.entry.args.lr": [0.001, 0.002],
                                "train.resources.constraint": ["a", "b"],
                            }
                        },
                    },
                )
            )

            batch = compile_stage_batch_for_kind(spec, kind="train")

            self.assertEqual(batch.stage_name, "train")
            self.assertEqual(len(batch.selected_runs), 4)
            self.assertEqual(sum(group.array_size for group in batch.group_plans), 4)
            self.assertEqual({group.resources["constraint"] for group in batch.group_plans}, {"a", "b"})

    def test_compile_pipeline_plan_keeps_train_and_eval_as_separate_batches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))

            plan = compile_pipeline_plan(spec)

            self.assertEqual(plan.stage_order, ("train", "eval"))
            self.assertEqual(set(plan.stage_batches), {"train", "eval"})
            self.assertEqual(plan.stage_batches["train"].stage_name, "train")
            self.assertEqual(plan.stage_batches["eval"].stage_name, "eval")
            self.assertNotEqual(
                plan.stage_batches["train"].submission_root,
                plan.stage_batches["eval"].submission_root,
            )

    def test_dry_run_audit_reports_unresolved_upstream_eval_as_valid_deferred_input(self) -> None:
        from slurmforge.planner import build_dry_run_audit

        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))
            batch = compile_stage_batch_for_kind(spec, kind="eval")

            audit = build_dry_run_audit(spec, batch, command="eval", full=False)

            self.assertEqual(audit.state, "valid")
            self.assertEqual(audit.plan_kind, "stage_batch")
            unresolved = audit.validation["unresolved_inputs"]
            self.assertEqual(len(unresolved), 1)
            self.assertTrue(unresolved[0]["deferred"])
