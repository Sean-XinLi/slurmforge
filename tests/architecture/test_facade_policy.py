from __future__ import annotations

import importlib

from tests.support.case import StageBatchSystemTestCase


class FacadePolicyTests(StageBatchSystemTestCase):
    def test_internal_package_facades_stay_empty(self) -> None:
        internal_packages = (
            "cli",
            "control",
            "docs_render",
            "emit",
            "executor",
            "inputs",
            "lineage",
            "materialization",
            "notifications",
            "orchestration",
            "outputs",
            "planner",
            "plans",
            "resolver",
            "resource_estimates",
            "root_model",
            "runtime",
            "sizing",
            "status",
            "storage",
            "submission",
        )
        for package in internal_packages:
            module = importlib.import_module(f"slurmforge.{package}")
            self.assertEqual(module.__all__, [], package)

    def test_nested_internal_facades_are_explicit_subsystem_entrypoints(self) -> None:
        allowed = {
            "slurmforge.emit.stage_render": (
                "render_stage_group_sbatch",
                "render_stage_notification_barrier_sbatch",
                "render_stage_notification_sbatch",
            ),
            "slurmforge.executor.launcher": ("build_shell_script",),
            "slurmforge.inputs.verification": ("record_for_binding",),
            "slurmforge.outputs.discovery": (
                "StageOutputDiscoveryResult",
                "discover_stage_outputs",
                "write_stage_outputs_record",
            ),
            "slurmforge.outputs.discovery.handlers": (
                "discover_file_output",
                "discover_files_output",
                "discover_manifest_output",
                "discover_metric_output",
            ),
            "slurmforge.planner.payloads": (
                "artifact_store_payload",
                "before_payload",
                "control_resources_payload",
                "default_bindings",
                "entry_payload",
                "environment_payload",
                "executor_runtime_payload",
                "launcher_payload",
                "notification_payload",
                "resource_payload",
                "resource_sizing_payload",
                "runtime_payload",
            ),
            "slurmforge.plans.serde": (
                "artifact_store_plan_from_dict",
                "before_step_plan_from_dict",
                "budget_plan_from_dict",
                "control_resources_plan_from_dict",
                "entry_plan_from_dict",
                "environment_plan_from_dict",
                "executor_runtime_plan_from_dict",
                "group_plan_from_dict",
                "launcher_plan_from_dict",
                "notification_plan_from_dict",
                "output_ref_from_dict",
                "python_runtime_plan_from_dict",
                "resource_plan_from_dict",
                "resource_sizing_from_dict",
                "runtime_plan_from_dict",
                "stage_batch_plan_from_dict",
                "stage_instance_plan_from_dict",
                "stage_outputs_record_from_dict",
                "train_eval_pipeline_plan_from_dict",
                "user_runtime_plan_from_dict",
            ),
            "slurmforge.resolver.explicit": (
                "explicit_input_bindings",
                "upstream_bindings_from_run",
                "upstream_bindings_from_stage_batch",
                "upstream_bindings_from_train_batch",
            ),
            "slurmforge.spec.stage_parse": ("parse_stage",),
            "slurmforge.starter.templates": (
                "EVAL_CHECKPOINT_TEMPLATE",
                "TRAIN_EVAL_TEMPLATE",
                "TRAIN_ONLY_TEMPLATE",
            ),
        }
        for module_name, expected_exports in allowed.items():
            module = importlib.import_module(module_name)
            self.assertEqual(tuple(module.__all__), expected_exports, module_name)
