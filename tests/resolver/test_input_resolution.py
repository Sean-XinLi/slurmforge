from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    resolve_stage_inputs_for_train_eval_pipeline,
    write_demo_project,
)
from tests.support.internal_records import write_train_eval_pipeline_layout
from tests.support.std import Path, tempfile, yaml


class InputResolutionTests(StageBatchSystemTestCase):
    def test_controller_dependency_resolution_is_contract_driven(self) -> None:
        controller_source = Path("src/slurmforge/controller/train_eval_pipeline.py").read_text(encoding="utf-8")
        self.assertNotIn("upstream_bindings_from_train_batch", controller_source)
        self.assertNotIn('kind="eval"', controller_source)
        self.assertNotIn("stage_spec.name == \"eval\"", controller_source)
        resolver_source = Path("src/slurmforge/resolver/__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("input_name == \"checkpoint\"", resolver_source)
        self.assertNotIn("_checkpoint_output", resolver_source)
        self.assertNotIn("outputs.get(\"checkpoint\")", resolver_source)
        self.assertFalse(Path("src/slurmforge/planner/input_resolution.py").exists())
        self.assertFalse(Path("src/slurmforge/planner/dependencies.py").exists())

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(plan.stage_batches["train"].submission_root), 1, 0), 0)

            resolved = resolve_stage_inputs_for_train_eval_pipeline(spec, plan, stage_name="eval")

            self.assertEqual(len(resolved.selected_runs), 1)
            self.assertEqual(resolved.blocked_run_ids, ())
            binding = resolved.input_bindings_by_run[resolved.selected_runs[0].run_id][0]
            self.assertEqual(binding.input_name, "checkpoint")
            self.assertEqual(binding.source.kind, "upstream_output")
            self.assertEqual(binding.source.output, "checkpoint")
            self.assertTrue(binding.resolved.path.endswith(".pt"))

    def test_eval_input_name_is_not_hardcoded_to_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"] = {
                "model_input": {
                    "source": {"kind": "upstream_output", "stage": "train", "output": "checkpoint"},
                    "expects": "path",
                    "required": True,
                    "inject": {"flag": "checkpoint_path", "env": "SFORGE_INPUT_CHECKPOINT"},
                }
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            write_train_eval_pipeline_layout(plan, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(plan.stage_batches["train"].submission_root), 1, 0), 0)

            resolved = resolve_stage_inputs_for_train_eval_pipeline(spec, plan, stage_name="eval")

            binding = resolved.input_bindings_by_run[resolved.selected_runs[0].run_id][0]
            self.assertEqual(binding.input_name, "model_input")
            self.assertEqual(binding.source.kind, "upstream_output")
            self.assertTrue(binding.resolved.path.endswith(".pt"))

    def test_checkpoint_source_requires_explicit_input_name_when_ambiguous(self) -> None:
        from slurmforge.spec.queries import stage_source_input_name

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            (root / "missing_checkpoint.pt").write_text("checkpoint", encoding="utf-8")
            (root / "missing_adapter.pt").write_text("adapter", encoding="utf-8")
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["depends_on"] = []
            payload["stages"]["eval"]["inputs"] = {
                "checkpoint": {
                    "source": {"kind": "external_path", "path": str(root / "missing_checkpoint.pt")},
                    "expects": "path",
                    "required": False,
                    "inject": {"flag": "checkpoint_path", "env": "SFORGE_INPUT_CHECKPOINT"},
                },
                "adapter": {
                    "source": {"kind": "external_path", "path": str(root / "missing_adapter.pt")},
                    "expects": "path",
                    "required": False,
                    "inject": {"flag": "adapter_path", "env": "SFORGE_INPUT_ADAPTER"},
                },
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            spec = load_experiment_spec(cfg_path)

            with self.assertRaisesRegex(Exception, "pass --input-name explicitly"):
                stage_source_input_name(spec, stage_name="eval")

    def test_checkpoint_cli_source_is_external_path_with_checkpoint_role(self) -> None:
        from slurmforge.orchestration import resolve_eval_inputs

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            checkpoint = root / "manual.pt"
            checkpoint.write_text("manual", encoding="utf-8")
            spec = load_experiment_spec(cfg_path)

            runs, bindings, source_ref = resolve_eval_inputs(
                spec,
                from_train_batch=None,
                from_run=None,
                checkpoint=str(checkpoint),
            )

            binding = bindings[runs[0].run_id][0]
            self.assertEqual(source_ref, f"checkpoint:{checkpoint.resolve()}")
            self.assertEqual(binding.source.kind, "external_path")
            self.assertEqual(binding.resolution["source_role"], "checkpoint")
            self.assertEqual(binding.resolved.path, str(checkpoint.resolve()))

    def test_checkpoint_cli_relative_path_resolves_from_config_root(self) -> None:
        from slurmforge.orchestration import resolve_eval_inputs

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            checkpoint = root / "manual.pt"
            checkpoint.write_text("manual", encoding="utf-8")
            spec = load_experiment_spec(cfg_path)

            runs, bindings, source_ref = resolve_eval_inputs(
                spec,
                from_train_batch=None,
                from_run=None,
                checkpoint="manual.pt",
            )

            binding = bindings[runs[0].run_id][0]
            self.assertEqual(source_ref, f"checkpoint:{checkpoint.resolve()}")
            self.assertEqual(binding.resolved.path, str(checkpoint.resolve()))
