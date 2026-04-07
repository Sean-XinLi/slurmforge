from __future__ import annotations

import unittest
from slurmforge.pipeline.launch import to_cli_args
from slurmforge.pipeline.launch.strategies import TorchrunStrategy


class RuntimeTests(unittest.TestCase):
    def test_torchrun_port_uses_offset(self) -> None:
        strategy = TorchrunStrategy()
        prefix, runtime = strategy.build_prefix(
            {
                "distributed": {
                    "nnodes": 1,
                    "nproc_per_node": 2,
                    "master_port": 29500,
                    "port_offset": 333,
                    "extra_torchrun_args": [],
                }
            },
            run_idx=7,
        )
        self.assertTrue(any(token.value == "--master_port=29840" for token in prefix))
        self.assertEqual(runtime.master_port, 29840)

    def test_to_cli_args_boolean_action_mapping(self) -> None:
        args = {
            "use_amp": True,
            "disable_cache": False,
            "feature_gate": False,
            "legacy_bool": True,
        }
        actions = {
            "use_amp": "store_true",
            "disable_cache": "store_false",
            "feature_gate": "booleanoptionalaction",
        }
        out = to_cli_args(args, arg_actions=actions)
        self.assertEqual(
            out,
            [
                "--use_amp",
                "--disable_cache",
                "--no-feature_gate",
                "--legacy_bool",
                "true",
            ],
        )

    def test_to_cli_args_boolean_optional_keeps_original_flag_shape(self) -> None:
        args = {"use_amp": False, "mixed_name": False}
        actions = {
            "use_amp": "booleanoptionalaction:--use_amp",
            "mixed_name": "booleanoptionalaction:--mixed-name",
        }
        out = to_cli_args(args, arg_actions=actions)
        self.assertEqual(out, ["--no-use_amp", "--no-mixed-name"])

    def test_to_cli_args_prefers_declared_canonical_flag(self) -> None:
        args = {
            "learning_rate": 0.001,
            "use_amp": True,
            "feature_gate": False,
        }
        actions = {
            "learning_rate": "value:--learning-rate",
            "use_amp": "store_true:--use-amp",
            "feature_gate": "booleanoptionalaction:--feature-gate",
        }
        out = to_cli_args(args, arg_actions=actions)
        self.assertEqual(out, ["--learning-rate", "0.001", "--use-amp", "--no-feature-gate"])

    def test_runtime_module_no_longer_exports_legacy_validation_helpers(self) -> None:
        import slurmforge.pipeline.launch as runtime

        self.assertFalse(hasattr(runtime, "validate_args_compat"))
        self.assertFalse(hasattr(runtime, "validate_runtime_within_allocation"))

    def test_runtime_module_no_longer_exports_legacy_runtime_resolution_helpers(self) -> None:
        import slurmforge.pipeline.launch as runtime

        for attr in (
            "resolve_launch_mode",
            "resolve_launcher_runtime",
            "normalize_runtime_for_launcher",
            "align_cluster_with_runtime",
            "allocation_from_cluster",
            "cap_estimate_to_resources",
            "cap_cluster_gpus",
        ):
            with self.subTest(attr=attr):
                self.assertFalse(hasattr(runtime, attr))

    def test_runtime_facade_no_longer_exports_planning_flavored_types(self) -> None:
        import slurmforge.pipeline.launch as runtime
        import slurmforge.pipeline.launch.strategies as runtime_launch

        for module in (runtime, runtime_launch):
            for attr in ("LaunchCapabilities", "SlurmAllocation"):
                with self.subTest(module=module.__name__, attr=attr):
                    self.assertFalse(hasattr(module, attr))
