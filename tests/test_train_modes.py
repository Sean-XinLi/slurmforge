from __future__ import annotations

import unittest
from pathlib import Path

from slurmforge.model_support.gpu_estimator import GpuEstimate
from slurmforge.model_support.catalog import ModelSpec
from slurmforge.pipeline.config.normalize import (
    normalize_cluster,
    normalize_launcher,
    normalize_resources,
    normalize_validation,
)
from slurmforge.pipeline.config.api import ExternalRuntimeConfig, RunConfigSpec
from slurmforge.pipeline.planning.train import CommandTrainStrategy, ModelCliTrainStrategy, TrainContext


class TrainModeTests(unittest.TestCase):
    def test_command_mode_builds_external_stage_plan(self) -> None:
        strategy = CommandTrainStrategy()
        base_ctx = dict(
            run_index=1,
            project_root=Path(".").resolve(),
            model_spec=ModelSpec("external", Path("train.py"), None, False, False, "default"),
            launcher_cfg=normalize_launcher({"distributed": {"nnodes": 1}, "workdir": "."}),
            cluster_cfg=normalize_cluster({}),
            launcher_nproc_per_node_explicit=False,
            cluster_nodes_explicit=False,
            cluster_gpus_per_node_explicit=False,
            resources_cfg=normalize_resources({"auto_gpu": True}),
            run_args={},
            model_overrides={},
            estimate=GpuEstimate(
                min_total_gpus=2,
                recommended_total_gpus=2,
                max_useful_total_gpus=2,
                estimated_vram_gb=1.0,
                reason="test",
            ),
            validation_cfg=normalize_validation({"cli_args": "warn"}),
        )
        ctx = TrainContext(
            run_spec=RunConfigSpec(
                mode="command",
                command="python3 train.py --name $USER",
                external_runtime=ExternalRuntimeConfig(nnodes=1, nproc_per_node=2),
            ),
            **base_ctx,
        )

        plan = strategy.build(ctx)

        self.assertEqual(plan.invocation_kind, "external_command")
        self.assertEqual(plan.launcher_kind, "external")
        self.assertEqual(plan.command_mode, "argv")
        self.assertIn("'$USER'", plan.command_text)
        self.assertEqual(plan.topology.processes_per_node, 2)
        self.assertEqual(plan.allocation.gpus_per_node, 2)

    def test_model_cli_strategy_resolves_multi_node_topology_from_cluster_hint(self) -> None:
        strategy = ModelCliTrainStrategy()
        ctx = TrainContext(
            run_index=1,
            project_root=Path(".").resolve(),
            run_spec=RunConfigSpec(mode="model_cli", args={}),
            model_spec=ModelSpec("demo", Path("train.py"), None, True, False, "default"),
            launcher_cfg=normalize_launcher({"mode": "auto", "distributed": {"nnodes": 1}, "workdir": "."}),
            cluster_cfg=normalize_cluster({"nodes": 2}),
            launcher_nproc_per_node_explicit=False,
            cluster_nodes_explicit=True,
            cluster_gpus_per_node_explicit=False,
            resources_cfg=normalize_resources({"auto_gpu": True, "max_available_gpus": 8}),
            run_args={},
            model_overrides={},
            estimate=GpuEstimate(
                min_total_gpus=4,
                recommended_total_gpus=4,
                max_useful_total_gpus=4,
                estimated_vram_gb=8.0,
                reason="test",
            ),
            validation_cfg=normalize_validation({"cli_args": "off"}),
        )

        plan = strategy.build(ctx)

        self.assertEqual(plan.launcher_kind, "ddp")
        self.assertEqual(plan.topology.nodes, 2)
        self.assertEqual(plan.topology.processes_per_node, 2)
        self.assertEqual(plan.allocation.nodes, 2)
        self.assertEqual(plan.allocation.gpus_per_node, 2)
