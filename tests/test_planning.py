from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from slurmforge.pipeline.compiler import AuthoringSourceRequest, compile_source
from slurmforge.pipeline.compiler.reports import (
    report_can_materialize,
    report_planned_run_count,
    report_total_failed_runs,
    report_total_runs,
    require_success,
)
from slurmforge.pipeline.config.normalize import normalize_launcher, normalize_validation
from slurmforge.pipeline.config.api import ExternalRuntimeConfig, EvalConfigSpec, build_experiment_spec
from slurmforge.pipeline.planning import (
    run_id,
    user_run_id_payload,
)
from slurmforge.pipeline.planning.contracts import (
    AllocationRequest,
    ExecutionTopology,
    ResourceEstimate,
    StageCapabilities,
    StageExecutionPlan,
)
from slurmforge.pipeline.planning.eval import build_eval_stage_plan
from slurmforge.pipeline.planning.validator import PlanningValidationError, validate_stage_execution_plan


class PlanningTests(unittest.TestCase):
    def _compile_cfg(self, cfg: dict, config_path: Path, *, project_root: Path, default_batch_name: str) -> object:
        config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        return compile_source(
            AuthoringSourceRequest(
                config_path=config_path,
                project_root=project_root,
                default_batch_name=default_batch_name,
            )
        )

    def test_run_id_uses_user_payload_only(self) -> None:
        project_root = Path(".").resolve()
        base_cfg = build_experiment_spec(
            {
                "project": "demo",
                "experiment_name": "exp",
                "model": {"name": "convbert", "script": "train.py"},
                "run": {"args": {"lr": 0.001}},
                "launcher": {"mode": "auto"},
            },
            project_root / "experiment.yaml",
            project_root=project_root,
        )
        extra_cfg = build_experiment_spec(
            {
                "project": "demo",
                "experiment_name": "exp",
                "model": {"name": "convbert", "script": "train.py"},
                "run": {"args": {"lr": 0.001}},
                "launcher": {"mode": "auto"},
                "cluster": {"account": "A100"},
                "env": {"modules": ["cuda/12.4.1"]},
            },
            project_root / "experiment.yaml",
            project_root=project_root,
        )
        self.assertEqual(
            run_id(user_run_id_payload(base_cfg, train_mode="model_cli", model_name="convbert", project_root=project_root)),
            run_id(user_run_id_payload(extra_cfg, train_mode="model_cli", model_name="convbert", project_root=project_root)),
        )

    def test_build_eval_stage_plan_inherits_train_topology_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            eval_script = tmp_path / "eval.py"
            eval_script.write_text("import argparse\nargparse.ArgumentParser().parse_args()\n", encoding="utf-8")
            train_stage = StageExecutionPlan(
                name="train",
                stage_kind="train",
                invocation_kind="model_cli",
                launcher_kind="ddp",
                command_text="torchrun train.py",
                workdir=tmp_path,
                topology=ExecutionTopology(nodes=2, processes_per_node=4, master_port=29500),
                allocation=AllocationRequest(nodes=2, gpus_per_node=4, cpus_per_task=8, mem="0"),
                estimate=ResourceEstimate(4, 8, 8, 80.0, "estimate"),
                capabilities=StageCapabilities(True, False, True, False, "cuda"),
                python_bin="python3",
                launcher_cfg=normalize_launcher(
                    {
                        "mode": "ddp",
                        "python_bin": "python3",
                        "workdir": ".",
                        "distributed": {
                            "nnodes": 2,
                            "nproc_per_node": 4,
                            "master_port": 29500,
                            "port_offset": 0,
                            "extra_torchrun_args": [],
                        },
                    }
                ),
                cluster_cfg={
                    "partition": "p",
                    "account": "a",
                    "qos": "q",
                    "time_limit": "01:00:00",
                    "nodes": 2,
                    "gpus_per_node": 4,
                    "cpus_per_task": 8,
                    "mem": "0",
                    "constraint": "",
                    "extra_sbatch_args": [],
                },
                script_path=tmp_path / "train.py",
                cli_args={},
                requested_launcher_mode="ddp",
                max_available_gpus_per_node=8,
            )

            eval_stage = build_eval_stage_plan(
                project_root=tmp_path,
                eval_spec=EvalConfigSpec(enabled=True, script="eval.py", args={}, pass_run_args=False),
                default_workdir=tmp_path,
                train_stage=train_stage,
                run_idx=0,
                validation_cfg=normalize_validation({"cli_args": "warn"}),
                run_args={},
                model_overrides={},
            )

        self.assertIsNotNone(eval_stage)
        assert eval_stage is not None
        self.assertEqual(eval_stage.topology.nodes, 2)
        self.assertEqual(eval_stage.topology.processes_per_node, 4)
        self.assertEqual(eval_stage.launcher_kind, "ddp")
        self.assertIn("torchrun", eval_stage.command_text)

    def test_build_eval_stage_plan_rejects_runtime_outside_train_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            eval_script = tmp_path / "eval.py"
            eval_script.write_text("import argparse\nargparse.ArgumentParser().parse_args()\n", encoding="utf-8")
            train_stage = StageExecutionPlan(
                name="train",
                stage_kind="train",
                invocation_kind="model_cli",
                launcher_kind="single",
                command_text="python3 train.py",
                workdir=tmp_path,
                topology=ExecutionTopology(nodes=1, processes_per_node=1, master_port=None),
                allocation=AllocationRequest(nodes=1, gpus_per_node=1, cpus_per_task=8, mem="0"),
                estimate=ResourceEstimate(1, 1, 1, 10.0, "estimate"),
                capabilities=StageCapabilities(True, False, True, False, "cuda"),
                python_bin="python3",
                launcher_cfg=normalize_launcher({"mode": "single", "python_bin": "python3", "workdir": "."}),
                cluster_cfg={
                    "partition": "p",
                    "account": "a",
                    "qos": "q",
                    "time_limit": "01:00:00",
                    "nodes": 1,
                    "gpus_per_node": 1,
                    "cpus_per_task": 8,
                    "mem": "0",
                    "constraint": "",
                    "extra_sbatch_args": [],
                },
                script_path=tmp_path / "train.py",
                cli_args={},
                requested_launcher_mode="single",
                max_available_gpus_per_node=8,
            )

            with self.assertRaises(PlanningValidationError):
                build_eval_stage_plan(
                    project_root=tmp_path,
                    eval_spec=EvalConfigSpec(
                        enabled=True,
                        script="eval.py",
                        launch_mode="ddp",
                        launcher=normalize_launcher({"distributed": {"nproc_per_node": 4}}),
                        args={},
                        pass_run_args=False,
                    ),
                    default_workdir=tmp_path,
                    train_stage=train_stage,
                    run_idx=0,
                    validation_cfg=normalize_validation({"cli_args": "warn"}),
                    run_args={},
                    model_overrides={},
                )

    def test_build_eval_stage_plan_uses_declared_external_runtime_for_eval_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            train_stage = StageExecutionPlan(
                name="train",
                stage_kind="train",
                invocation_kind="model_cli",
                launcher_kind="ddp",
                command_text="torchrun train.py",
                workdir=tmp_path,
                topology=ExecutionTopology(nodes=2, processes_per_node=4, master_port=29500),
                allocation=AllocationRequest(nodes=2, gpus_per_node=4, cpus_per_task=8, mem="0"),
                estimate=ResourceEstimate(4, 8, 8, 80.0, "estimate"),
                capabilities=StageCapabilities(True, False, True, False, "cuda"),
                python_bin="python3",
                launcher_cfg=normalize_launcher(
                    {
                        "mode": "ddp",
                        "python_bin": "python3",
                        "workdir": ".",
                        "distributed": {
                            "nnodes": 2,
                            "nproc_per_node": 4,
                            "master_port": 29500,
                            "port_offset": 0,
                            "extra_torchrun_args": [],
                        },
                    }
                ),
                cluster_cfg={
                    "partition": "p",
                    "account": "a",
                    "qos": "q",
                    "time_limit": "01:00:00",
                    "nodes": 2,
                    "gpus_per_node": 4,
                    "cpus_per_task": 8,
                    "mem": "0",
                    "constraint": "",
                    "extra_sbatch_args": [],
                },
                script_path=tmp_path / "train.py",
                cli_args={},
                requested_launcher_mode="ddp",
                max_available_gpus_per_node=8,
            )

            eval_stage = build_eval_stage_plan(
                project_root=tmp_path,
                eval_spec=EvalConfigSpec(
                    enabled=True,
                    command="bash launch_eval.sh",
                    command_mode="argv",
                    external_runtime=ExternalRuntimeConfig(nnodes=2, nproc_per_node=4),
                    pass_run_args=False,
                ),
                default_workdir=tmp_path,
                train_stage=train_stage,
                run_idx=0,
                validation_cfg=normalize_validation({"cli_args": "warn"}),
                run_args={},
                model_overrides={},
            )

        self.assertIsNotNone(eval_stage)
        assert eval_stage is not None
        self.assertEqual(eval_stage.invocation_kind, "external_command")
        self.assertEqual(eval_stage.launcher_kind, "external")
        self.assertEqual(eval_stage.command_mode, "argv")
        self.assertEqual(eval_stage.topology.nodes, 2)
        self.assertEqual(eval_stage.topology.processes_per_node, 4)

    def test_build_eval_stage_plan_preserves_raw_external_command_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            train_stage = StageExecutionPlan(
                name="train",
                stage_kind="train",
                invocation_kind="model_cli",
                launcher_kind="single",
                command_text="python3 train.py",
                workdir=tmp_path,
                topology=ExecutionTopology(nodes=1, processes_per_node=1, master_port=None),
                allocation=AllocationRequest(nodes=1, gpus_per_node=1, cpus_per_task=8, mem="0"),
                estimate=ResourceEstimate(1, 1, 1, 10.0, "estimate"),
                capabilities=StageCapabilities(True, False, True, False, "cuda"),
                python_bin="python3",
                launcher_cfg=normalize_launcher({"mode": "single", "python_bin": "python3", "workdir": "."}),
                cluster_cfg={
                    "partition": "p",
                    "account": "a",
                    "qos": "q",
                    "time_limit": "01:00:00",
                    "nodes": 1,
                    "gpus_per_node": 1,
                    "cpus_per_task": 8,
                    "mem": "0",
                    "constraint": "",
                    "extra_sbatch_args": [],
                },
                script_path=tmp_path / "train.py",
                cli_args={},
                requested_launcher_mode="single",
                max_available_gpus_per_node=8,
            )

            eval_stage = build_eval_stage_plan(
                project_root=tmp_path,
                eval_spec=EvalConfigSpec(
                    enabled=True,
                    command="bash eval.sh | sed 's/x/y/'",
                    command_mode="raw",
                    external_runtime=ExternalRuntimeConfig(nnodes=1, nproc_per_node=1),
                    pass_run_args=False,
                ),
                default_workdir=tmp_path,
                train_stage=train_stage,
                run_idx=0,
                validation_cfg=normalize_validation({"cli_args": "warn"}),
                run_args={},
                model_overrides={},
            )

        assert eval_stage is not None
        self.assertEqual(eval_stage.command_mode, "raw")
        self.assertEqual(eval_stage.command_text, "bash eval.sh | sed 's/x/y/'")

    def test_resource_warnings_policy_controls_all_resource_categories(self) -> None:
        plan = StageExecutionPlan(
            name="train",
            stage_kind="train",
            invocation_kind="external_command",
            launcher_kind="single",
            command_text="python3 train.py",
            workdir=Path(".").resolve(),
            topology=ExecutionTopology(nodes=1, processes_per_node=1, master_port=None),
            allocation=AllocationRequest(nodes=2, gpus_per_node=4, cpus_per_task=0, mem="0"),
            estimate=ResourceEstimate(1, 1, 1, 8.0, "estimate"),
            capabilities=StageCapabilities(True, False, True, True, "cuda"),
            python_bin="python3",
            launcher_cfg=normalize_launcher({"mode": "single", "python_bin": "python3", "workdir": "."}),
            cluster_cfg={
                "partition": "p",
                "account": "a",
                "qos": "q",
                "time_limit": "01:00:00",
                "nodes": 2,
                "gpus_per_node": 4,
                "cpus_per_task": 0,
                "mem": "0",
                "constraint": "",
                "extra_sbatch_args": [],
            },
            script_path=None,
            cli_args={},
            requested_launcher_mode="external",
            max_available_gpus_per_node=8,
        )

        suppressed = validate_stage_execution_plan(
            plan,
            normalize_validation({"resource_warnings": "off", "topology_errors": "error"}),
        )
        self.assertFalse(any(item.category == "resource" and item.severity == "warning" for item in suppressed.diagnostics))

        with self.assertRaises(PlanningValidationError) as raised:
            validate_stage_execution_plan(
                plan,
                normalize_validation({"resource_warnings": "error", "topology_errors": "error"}),
            )
        self.assertTrue(any(item.category == "resource" and item.severity == "error" for item in raised.exception.diagnostics))

    def test_compile_batch_populates_stage_plans_and_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            train_script = tmp_path / "train.py"
            train_script.write_text(
                "import argparse\nparser = argparse.ArgumentParser()\nparser.add_argument('--lr')\nparser.parse_args()\n",
                encoding="utf-8",
            )
            cfg = {
                "project": "demo",
                "experiment_name": "exp",
                "model": {"name": "custom", "script": "train.py"},
                "run": {"args": {"lr": 0.001}},
                "launcher": {"mode": "single"},
                "cluster": {"gpus_per_node": 2},
            }
            report = self._compile_cfg(
                cfg,
                tmp_path / "experiment.yaml",
                project_root=tmp_path,
                default_batch_name="test",
            )
            planned_batch = require_success(report)

        self.assertEqual(planned_batch.total_runs, 1)
        plan = planned_batch.planned_runs[0].plan
        self.assertEqual(plan.train_stage.stage_kind, "train")
        self.assertTrue(any(item.code == "single_launcher_multigpu_allocation" for item in plan.planning_diagnostics))

    def test_compile_planned_batch_collects_failures_without_stopping_at_first_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            train_script = tmp_path / "train.py"
            train_script.write_text(
                "import argparse\nparser = argparse.ArgumentParser()\nparser.add_argument('--lr')\nparser.parse_args()\n",
                encoding="utf-8",
            )
            cfg = {
                "project": "demo",
                "experiment_name": "exp",
                "model": {"name": "custom", "script": "train.py", "ddp_supported": True},
                "run": {"args": {"lr": 0.001}},
                "launcher": {"mode": "ddp", "distributed": {"nproc_per_node": 2}},
                "cluster": {"nodes": 1},
                "sweep": {
                    "enabled": True,
                    "shared_axes": {"cluster.gpus_per_node": [1, 2]},
                },
            }
            report = self._compile_cfg(
                cfg,
                tmp_path / "experiment.yaml",
                project_root=tmp_path,
                default_batch_name="test",
            )

        self.assertEqual(report_total_runs(report), 2)
        self.assertEqual(report_total_failed_runs(report), 1)
        self.assertEqual(report_planned_run_count(report), 1)
        self.assertFalse(report_can_materialize(report))
        self.assertTrue(
            any(
                item.code == "runtime_processes_exceed_gpus"
                for failed_run in report.failed_runs
                for item in failed_run.diagnostics
            )
        )
