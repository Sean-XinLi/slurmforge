from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.std import Path, tempfile, yaml


class PlannerTests(StageBatchSystemTestCase):
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
                            }
                        },
                    },
                )
            )

            batch = compile_stage_batch_for_kind(spec, kind="train")

            self.assertEqual(batch.stage_name, "train")
            self.assertEqual(len(batch.selected_runs), 4)
            self.assertEqual(sum(group.array_size for group in batch.group_plans), 4)
            self.assertEqual({group.resources.constraint for group in batch.group_plans}, {"a", "b"})

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
            self.assertEqual({group.resources.constraint for group in batch.group_plans}, {"a", "b"})

    def test_compile_stage_batch_materializes_auto_gpu_sizing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "hardware": {
                            "gpu_types": {
                                "a100_80gb": {
                                    "memory_gb": 80,
                                    "usable_memory_fraction": 0.90,
                                    "max_gpus_per_node": 8,
                                    "slurm": {"constraint": "a100"},
                                }
                            }
                        },
                        "sizing": {"gpu": {"defaults": {"safety_factor": 1.15}}},
                        "dispatch": {"max_available_gpus": 8, "overflow_policy": "serialize_groups"},
                    },
                )
            )
            payload = yaml.safe_load(spec.config_path.read_text())
            payload["stages"]["train"]["resources"]["gpu_type"] = "a100_80gb"
            payload["stages"]["train"]["resources"]["gpus_per_node"] = "auto"
            payload["stages"]["train"]["resources"].pop("constraint", None)
            payload["stages"]["train"]["gpu_sizing"] = {
                "estimator": "heuristic",
                "target_memory_gb": 192,
                "min_gpus_per_job": 4,
                "max_gpus_per_job": 4,
            }
            spec.config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            spec = load_experiment_spec(spec.config_path)

            batch = compile_stage_batch_for_kind(spec, kind="train")
            instance = batch.stage_instances[0]

            self.assertEqual(instance.resources.gpus_per_node, 4)
            self.assertEqual(instance.resources.constraint, "a100")
            self.assertEqual(instance.resource_sizing.mode, "auto")
            self.assertEqual(instance.resource_sizing.resolved_total_gpus, 4)
            self.assertEqual(batch.group_plans[0].gpus_per_task, 4)

    def test_compile_train_eval_pipeline_plan_keeps_train_and_eval_as_separate_batches(self) -> None:
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

            full_audit = build_dry_run_audit(spec, batch, command="eval", full=True)
            self.assertIn("stages", full_audit.resource_estimate)
            self.assertEqual(full_audit.resource_estimate["stages"][0]["stage_name"], "eval")
