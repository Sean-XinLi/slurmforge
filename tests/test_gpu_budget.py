"""Tests for the GPU budget planner and its integration into materialization.

Covers the spec's 13-test checklist:

 1.  max_available_gpus no longer caps max_gpus_per_job
 2.  topology uses max_gpus_per_job, not max_available_gpus
 3.  single-group sbatch emits ``--array=0-N%K``
 4.  multi-group within budget uses shared throttle distribution
 5.  multi-group over budget with policy=error fails
 6.  multi-group over budget with policy=serial emits dependency chain
 7.  serial dependency uses afterany + --kill-on-invalid-dep=no
 8.  policy=best_effort does not serialize, does not fail, but warns
 9.  single-task gpus_per_task > max_available_gpus is a hard error
10.  gpus_per_task <= 0 is rejected (planning/internal)
11.  dry-run prints limiting_run / limiting_model / max_estimated_gpus
12.  throttle greater than Slurm MaxArraySize is clamped with a warning
13.  manifest persists the stable gpu_budget_plan schema
"""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path

from slurmforge.errors import ConfigContractError, PlanningError
from slurmforge.pipeline.compiler import AuthoringSourceRequest, compile_source
from slurmforge.pipeline.compiler.api import iter_compile_report_lines
from slurmforge.pipeline.compiler.reports import (
    BatchCompileReport,
    report_has_failures,
    report_warning_count,
    require_success,
)
from slurmforge.pipeline.config.normalize import (
    normalize_cluster,
    normalize_dispatch,
    normalize_resources,
)
from slurmforge.pipeline.config.runtime import DispatchConfig
from slurmforge.pipeline.launch import max_gpus_per_job
from slurmforge.pipeline.materialization import materialize_batch, print_dry_run_batch
from slurmforge.pipeline.planning import (
    BatchIdentity,
    PlannedBatch,
    PlannedRun,
    plan_gpu_budget,
    serialize_gpu_budget_plan,
)
from slurmforge.pipeline.planning.contracts import AllocationRequest, ExecutionTopology, PlanDiagnostic
from slurmforge.pipeline.planning.gpu_budget import DEFAULT_SLURM_MAX_ARRAY_SIZE
from slurmforge.storage.backends.filesystem import FileSystemPlanningStore
from tests._support import (
    make_template_env,
    sample_run_plan,
    sample_run_snapshot,
    sample_stage_plan,
)


def _planned_run(
    *,
    run_index: int,
    run_id: str,
    model_name: str,
    cluster_overrides: dict,
    allocation: AllocationRequest,
    recommended_total_gpus: int,
    batch_root: Path,
    total_runs: int = 1,
) -> PlannedRun:
    base_stage = sample_stage_plan()
    cluster = normalize_cluster({**_base_cluster_dict(), **cluster_overrides})
    train_stage = replace(
        base_stage,
        cluster_cfg=cluster,
        allocation=allocation,
        topology=ExecutionTopology(
            nodes=allocation.nodes,
            processes_per_node=allocation.gpus_per_node,
            master_port=None,
        ),
        estimate=replace(
            base_stage.estimate,
            recommended_total_gpus=recommended_total_gpus,
            max_useful_total_gpus=recommended_total_gpus,
        ),
    )
    plan = sample_run_plan(
        run_index=run_index,
        total_runs=total_runs,
        run_id=run_id,
        model_name=model_name,
        train_stage=train_stage,
        cluster=cluster,
        run_dir=str(batch_root / "runs" / f"run_{run_index:03d}_{run_id}"),
        run_dir_rel=f"runs/run_{run_index:03d}_{run_id}",
    )
    snapshot = sample_run_snapshot(
        run_index=plan.run_index,
        total_runs=plan.total_runs,
        run_id=plan.run_id,
    )
    return PlannedRun(plan=plan, snapshot=snapshot)


def _finalize_total_runs(runs: list[PlannedRun]) -> list[PlannedRun]:
    """Stamp the correct ``total_runs`` onto every PlannedRun's plan & snapshot."""
    total = len(runs)
    fixed: list[PlannedRun] = []
    for pr in runs:
        fixed.append(
            PlannedRun(
                plan=replace(pr.plan, total_runs=total),
                snapshot=replace(pr.snapshot, total_runs=total),
            )
        )
    return fixed


def _base_cluster_dict() -> dict:
    return {
        "partition": "gpu",
        "account": "acct",
        "qos": "normal",
        "time_limit": "01:00:00",
        "nodes": 1,
        "gpus_per_node": 1,
        "cpus_per_task": 4,
        "mem": "0",
        "constraint": "",
        "extra_sbatch_args": [],
    }


def _batch_identity(batch_root: Path) -> BatchIdentity:
    return BatchIdentity(
        project_root=batch_root.parent.parent.parent.parent,
        base_output_dir=batch_root.parent.parent.parent,
        project="demo",
        experiment_name="exp",
        batch_name=batch_root.name[len("batch_"):] if batch_root.name.startswith("batch_") else batch_root.name,
    )


# ---------------------------------------------------------------------------
# 1. max_available_gpus no longer caps max_gpus_per_job
# ---------------------------------------------------------------------------


class DecoupledSemanticsTests(unittest.TestCase):
    def test_max_available_gpus_does_not_cap_max_gpus_per_job(self) -> None:
        cfg = normalize_resources({"max_available_gpus": 4, "max_gpus_per_job": 16})
        self.assertEqual(cfg.max_available_gpus, 4)
        self.assertEqual(cfg.max_gpus_per_job, 16)

    def test_max_gpus_per_job_helper_reads_independent_field(self) -> None:
        cfg = normalize_resources({"max_available_gpus": 4, "max_gpus_per_job": 7})
        self.assertEqual(max_gpus_per_job(cfg), 7)

    def test_topology_uses_max_gpus_per_job_for_per_node_fallback(self) -> None:
        """topology.per_node_limit falls back to max_gpus_per_job when cluster.gpus_per_node is 'auto'.

        This is an integration-style check: rather than import the topology
        resolver and fabricate a train context, we assert the helper the
        resolver calls returns max_gpus_per_job (not max_available_gpus).
        """
        cfg = normalize_resources({"max_available_gpus": 32, "max_gpus_per_job": 4})
        self.assertEqual(max_gpus_per_job(cfg), 4)


# ---------------------------------------------------------------------------
# Helpers to exercise plan_gpu_budget directly.
# ---------------------------------------------------------------------------


def _build_budget(runs: list[PlannedRun], *, max_available_gpus: int, policy: str = "error"):
    return plan_gpu_budget(
        runs,
        max_available_gpus=max_available_gpus,
        dispatch_cfg=DispatchConfig(group_overflow_policy=policy),
    )


# ---------------------------------------------------------------------------
# 3 & 4. Single-group throttle + multi-group shared budget allocation
# ---------------------------------------------------------------------------


class ThrottleAllocationTests(unittest.TestCase):
    def _build_runs(self, cluster_kv: list[tuple[int, int]], *, batch_root: Path) -> list[PlannedRun]:
        # cluster_kv: list of (gpus_per_node, task_count).  Each entry becomes
        # one group; tasks within the group share the same cluster and env.
        runs: list[PlannedRun] = []
        run_index = 1
        for group_idx, (gpus_per_node, task_count) in enumerate(cluster_kv, start=1):
            for _ in range(task_count):
                allocation = AllocationRequest(
                    nodes=1, gpus_per_node=gpus_per_node, cpus_per_task=4, mem="0",
                )
                runs.append(
                    _planned_run(
                        run_index=run_index,
                        run_id=f"r{run_index:03d}",
                        model_name=f"model_g{group_idx}",
                        cluster_overrides={
                            "gpus_per_node": gpus_per_node,
                            "partition": f"gpu_g{group_idx}",  # force distinct group
                        },
                        allocation=allocation,
                        recommended_total_gpus=gpus_per_node,
                        batch_root=batch_root,
                    )
                )
                run_index += 1
        return _finalize_total_runs(runs)

    def test_single_group_shared_budget_fills_throttle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._build_runs([(2, 10)], batch_root=batch_root)
            plan = _build_budget(runs, max_available_gpus=16)
            self.assertEqual(plan.policy_applied, "shared_budget")
            self.assertEqual(len(plan.groups), 1)
            self.assertEqual(plan.groups[0].throttle, 8)  # floor(16/2) = 8
            self.assertEqual(plan.groups[0].gpus_per_task, 2)
            self.assertEqual(plan.groups[0].max_group_gpus, 16)
            self.assertTrue(plan.strict_global_limit)

    def test_multi_group_shared_budget_prefers_smaller_gpus_per_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            # group A: 8 GPUs/task × 10 tasks; group B: 1 GPU/task × 20 tasks
            # min concurrent = 8 + 1 = 9, budget 16, 7 left → all go to B
            runs = self._build_runs([(8, 10), (1, 20)], batch_root=batch_root)
            plan = _build_budget(runs, max_available_gpus=16)
            self.assertEqual(plan.policy_applied, "shared_budget")
            by_gpt = {g.gpus_per_task: g for g in plan.groups}
            self.assertEqual(by_gpt[8].throttle, 1)  # starved — budget went to smaller task
            self.assertEqual(by_gpt[1].throttle, 8)
            self.assertTrue(by_gpt[8].constrained)
            self.assertEqual(by_gpt[8].constraint_reason, "budget_exhausted")


# ---------------------------------------------------------------------------
# 5 & 6 & 7. Policy=error fails; policy=serial emits dependency chain w/ afterany + kill-flag
# ---------------------------------------------------------------------------


class OverflowPolicyTests(unittest.TestCase):
    def _overflow_runs(self, batch_root: Path) -> list[PlannedRun]:
        # 3 distinct groups, each needs 8 GPU/task minimum. Budget = 16.
        runs: list[PlannedRun] = []
        for group_idx in range(1, 4):
            runs.append(
                _planned_run(
                    run_index=group_idx,
                    run_id=f"r{group_idx}",
                    model_name=f"model_g{group_idx}",
                    cluster_overrides={
                        "gpus_per_node": 8,
                        "partition": f"gpu_g{group_idx}",
                    },
                    allocation=AllocationRequest(nodes=1, gpus_per_node=8, cpus_per_task=4, mem="0"),
                    recommended_total_gpus=8,
                    batch_root=batch_root,
                )
            )
        return _finalize_total_runs(runs)

    def test_error_policy_raises_on_overflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._overflow_runs(batch_root)
            with self.assertRaises(ConfigContractError) as ctx:
                _build_budget(runs, max_available_gpus=16, policy="error")
            self.assertIn("exceeds resources.max_available_gpus", str(ctx.exception))

    def test_serial_policy_emits_afterany_and_kill_on_invalid_dep(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._overflow_runs(batch_root)
            plan = _build_budget(runs, max_available_gpus=16, policy="serial")
            self.assertEqual(plan.policy_applied, "serialized_groups")
            self.assertFalse(plan.strict_global_limit is False)  # single-group-at-a-time => strict holds
            self.assertTrue(plan.strict_global_limit)

            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=16,
                dispatch_cfg=DispatchConfig(group_overflow_policy="serial"),
                gpu_budget_plan=plan,
            )
            result = materialize_batch(
                planned_batch=planned_batch,
                planning_store=FileSystemPlanningStore(make_template_env()),
            )
            submit_text = result.submit_script.read_text(encoding="utf-8")
            # Dependency chain
            self.assertIn("--dependency=afterany:${GROUP_PREV_JOB_ID}", submit_text)
            self.assertIn("--kill-on-invalid-dep=no", submit_text)
            # The first group must NOT have --dependency; two dependency flags for three groups
            self.assertEqual(submit_text.count("--dependency=afterany:${GROUP_PREV_JOB_ID}"), 2)

    def test_serial_merges_external_and_serial_dependencies_into_one_flag(self) -> None:
        """Slurm treats multiple --dependency flags as replacement, not merge.
        When serial chaining is active and the user supplies external
        submit_dependencies, the produced sbatch call must contain exactly
        ONE --dependency flag whose value holds both clauses.
        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._overflow_runs(batch_root)
            plan = _build_budget(runs, max_available_gpus=16, policy="serial")
            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=16,
                dispatch_cfg=DispatchConfig(group_overflow_policy="serial"),
                gpu_budget_plan=plan,
                submit_dependencies={"afterok": ["101", "202"], "afterany": ["303"]},
            )
            result = materialize_batch(
                planned_batch=planned_batch,
                planning_store=FileSystemPlanningStore(make_template_env()),
            )
            submit_text = result.submit_script.read_text(encoding="utf-8")

            # Structural: no sbatch invocation may carry two --dependency flags.
            for line in submit_text.splitlines():
                if line.lstrip().startswith("JOB_ID=$(sbatch"):
                    self.assertLessEqual(
                        line.count("--dependency="),
                        1,
                        f"multiple --dependency flags on one sbatch call: {line!r}",
                    )

            # First group: external deps only, no serial clause, no kill-flag.
            self.assertIn(
                'sbatch --parsable --dependency=afterany:303,afterok:101:202 ',
                submit_text,
            )
            # Subsequent groups: external + serial clause merged into ONE flag,
            # plus --kill-on-invalid-dep=no (only for serial).
            self.assertIn(
                "--dependency=afterany:303,afterok:101:202,afterany:${GROUP_PREV_JOB_ID}",
                submit_text,
            )
            # The kill-on-invalid-dep flag must only accompany groups that
            # actually carry the serial clause.
            self.assertEqual(
                submit_text.count("--kill-on-invalid-dep=no"),
                submit_text.count("afterany:${GROUP_PREV_JOB_ID}"),
            )

    def test_shared_budget_external_deps_still_use_single_flag(self) -> None:
        """Regression guard: without serial chaining, external deps still emit
        exactly one --dependency flag per sbatch call (no duplication)."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            # Runs that fit in the shared budget — no serial chain applied.
            runs = _finalize_total_runs([
                _planned_run(
                    run_index=1,
                    run_id="r1",
                    model_name="m",
                    cluster_overrides={"gpus_per_node": 2},
                    allocation=AllocationRequest(nodes=1, gpus_per_node=2, cpus_per_task=4, mem="0"),
                    recommended_total_gpus=2,
                    batch_root=batch_root,
                ),
            ])
            plan = _build_budget(runs, max_available_gpus=16)
            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=16,
                dispatch_cfg=DispatchConfig(),
                gpu_budget_plan=plan,
                submit_dependencies={"afterok": ["101"]},
            )
            result = materialize_batch(
                planned_batch=planned_batch,
                planning_store=FileSystemPlanningStore(make_template_env()),
            )
            submit_text = result.submit_script.read_text(encoding="utf-8")
            for line in submit_text.splitlines():
                if line.lstrip().startswith("JOB_ID=$(sbatch"):
                    self.assertEqual(line.count("--dependency="), 1, f"bad: {line!r}")
                    self.assertNotIn("afterany:${GROUP_PREV_JOB_ID}", line)
                    self.assertNotIn("--kill-on-invalid-dep=no", line)

    def test_best_effort_does_not_serialize_but_warns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._overflow_runs(batch_root)
            plan = _build_budget(runs, max_available_gpus=16, policy="best_effort")
            self.assertEqual(plan.policy_applied, "best_effort")
            self.assertFalse(plan.strict_global_limit)
            codes = {w.code for w in plan.warnings}
            self.assertIn("best_effort_no_strict_global_limit", codes)


# ---------------------------------------------------------------------------
# 9 & 10. Single-task > budget is hard error; gpus_per_task <= 0 is hard error
# ---------------------------------------------------------------------------


class HardErrorTests(unittest.TestCase):
    def test_single_task_exceeds_budget_is_hard_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = [
                _planned_run(
                    run_index=1,
                    run_id="huge",
                    model_name="llama70b",
                    cluster_overrides={"gpus_per_node": 32},
                    allocation=AllocationRequest(nodes=1, gpus_per_node=32, cpus_per_task=4, mem="0"),
                    recommended_total_gpus=32,
                    batch_root=batch_root,
                ),
            ]
            with self.assertRaises(ConfigContractError) as ctx:
                _build_budget(runs, max_available_gpus=16, policy="serial")
            self.assertIn("exceeds resources.max_available_gpus", str(ctx.exception))

    def test_zero_gpus_per_task_is_rejected_at_allocation_level(self) -> None:
        # Upstream defense: AllocationRequest itself refuses gpus_per_node < 1,
        # so CPU-only plans cannot reach the budget planner.
        with self.assertRaises(PlanningError):
            AllocationRequest(nodes=1, gpus_per_node=0, cpus_per_task=4, mem="0")

    def test_zero_gpus_per_task_is_rejected_by_budget_planner(self) -> None:
        # Defense-in-depth: even if a caller somehow synthesized a zero-GPU
        # group bypassing AllocationRequest (e.g. internal bug), the budget
        # planner rejects it with a PlanningError.
        from slurmforge.pipeline.planning.gpu_budget import _RawGroup, _validate_group_budget

        bad = _RawGroup(
            group_id=1,
            signature="synthetic",
            run_ids=["r1"],
            run_models=["m"],
            run_estimated_gpus=[0],
            gpus_per_task=0,
            first_seen_index=0,
        )
        with self.assertRaises(PlanningError) as ctx:
            _validate_group_budget(bad, max_available_gpus=16)
        self.assertIn("gpus_per_task", str(ctx.exception))


# ---------------------------------------------------------------------------
# 3 (sbatch %K) + 11 (dry-run) + 13 (manifest) + 12 (MaxArraySize clamp)
# ---------------------------------------------------------------------------


class MaterializationTests(unittest.TestCase):
    def _run_pair(self, batch_root: Path) -> list[PlannedRun]:
        runs = [
            _planned_run(
                run_index=i + 1,
                run_id=f"r{i+1}",
                model_name=f"m{i+1}",
                cluster_overrides={"gpus_per_node": 2},
                allocation=AllocationRequest(nodes=1, gpus_per_node=2, cpus_per_task=4, mem="0"),
                recommended_total_gpus=2,
                batch_root=batch_root,
            )
            for i in range(10)
        ]
        return _finalize_total_runs(runs)

    def test_sbatch_emits_array_throttle_percent_k(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._run_pair(batch_root)
            plan = _build_budget(runs, max_available_gpus=8)  # throttle = min(10, 8/2=4) = 4
            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=8,
                dispatch_cfg=DispatchConfig(),
                gpu_budget_plan=plan,
            )
            result = materialize_batch(
                planned_batch=planned_batch,
                planning_store=FileSystemPlanningStore(make_template_env()),
            )
            sbatch_text = Path(result.array_groups_meta[0]["sbatch_path"]).read_text()
            self.assertIn("#SBATCH --array=0-9%4", sbatch_text)

    def test_sbatch_keeps_array_and_mem_on_separate_lines(self) -> None:
        """Regression guard: template whitespace handling must not fuse two #SBATCH lines.

        Historical bug: a Jinja trim marker on the array conditional ate the
        trailing newline so the next `#SBATCH --mem=...` line got appended:
            #SBATCH --array=0-9%4#SBATCH --mem=8G
        which Slurm rejects.  The template ``trim_blocks=True`` env setting
        eats ANY newline after ``{% endif %}``, so the conditional must NOT
        end with a block tag — it uses a ``{{ ... if ... else ... }}`` expression.
        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            # Force cluster.mem != "0" so the mem line actually renders.
            runs: list[PlannedRun] = []
            for i in range(10):
                runs.append(
                    _planned_run(
                        run_index=i + 1,
                        run_id=f"r{i+1}",
                        model_name=f"m{i+1}",
                        cluster_overrides={"gpus_per_node": 2, "mem": "8G"},
                        allocation=AllocationRequest(nodes=1, gpus_per_node=2, cpus_per_task=4, mem="8G"),
                        recommended_total_gpus=2,
                        batch_root=batch_root,
                    )
                )
            runs = _finalize_total_runs(runs)
            plan = _build_budget(runs, max_available_gpus=8)  # throttle=4, array_size=10
            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=8,
                dispatch_cfg=DispatchConfig(),
                gpu_budget_plan=plan,
            )
            result = materialize_batch(
                planned_batch=planned_batch,
                planning_store=FileSystemPlanningStore(make_template_env()),
            )
            sbatch_text = Path(result.array_groups_meta[0]["sbatch_path"]).read_text()
            self.assertIn("#SBATCH --array=0-9%4", sbatch_text)
            self.assertIn("#SBATCH --mem=8G", sbatch_text)
            # Structural guard: no rendered line may contain "#SBATCH" more than once.
            # This catches any future template whitespace/trim bug that fuses directives.
            for line_no, line in enumerate(sbatch_text.splitlines(), start=1):
                self.assertLessEqual(
                    line.count("#SBATCH"),
                    1,
                    f"line {line_no} contains multiple #SBATCH directives: {line!r}",
                )

    def test_dry_run_shows_limiting_run_model_and_max_estimated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._run_pair(batch_root)
            # Bump run 3's recommended_total_gpus so it becomes limiting.
            runs[2] = replace(
                runs[2],
                plan=replace(
                    runs[2].plan,
                    train_stage=replace(
                        runs[2].plan.train_stage,
                        estimate=replace(
                            runs[2].plan.train_stage.estimate,
                            recommended_total_gpus=6,
                            max_useful_total_gpus=6,
                        ),
                    ),
                ),
            )
            plan = _build_budget(runs, max_available_gpus=8)
            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=8,
                dispatch_cfg=DispatchConfig(),
                gpu_budget_plan=plan,
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                print_dry_run_batch(planned_batch)
            out = buf.getvalue()
            self.assertIn("GPU batch budget", out)
            self.assertIn("limiting_run=r3", out)
            self.assertIn("limiting_model=m3", out)
            self.assertIn("max_estimated_gpus=6", out)

    def test_manifest_persists_stable_gpu_budget_plan_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = self._run_pair(batch_root)
            plan = _build_budget(runs, max_available_gpus=8)
            planned_batch = PlannedBatch(
                identity=_batch_identity(batch_root),
                planned_runs=tuple(runs),
                max_available_gpus=8,
                dispatch_cfg=DispatchConfig(),
                gpu_budget_plan=plan,
            )
            result = materialize_batch(
                planned_batch=planned_batch,
                planning_store=FileSystemPlanningStore(make_template_env()),
            )
            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertIn("gpu_budget_plan", manifest)
            budget = manifest["gpu_budget_plan"]
            required = {
                "max_available_gpus",
                "group_overflow_policy",
                "policy_applied",
                "min_concurrent_gpus",
                "max_planned_concurrent_gpus",
                "strict_global_limit",
                "groups",
                "warnings",
            }
            self.assertEqual(required - budget.keys(), set())
            group = budget["groups"][0]
            group_fields = {
                "group_id",
                "task_count",
                "gpus_per_task",
                "throttle",
                "max_group_gpus",
                "limiting_run",
                "limiting_model",
                "max_estimated_gpus",
                "constrained",
                "constraint_reason",
            }
            self.assertEqual(group_fields - group.keys(), set())


# ---------------------------------------------------------------------------
# 12. MaxArraySize clamp warning
# ---------------------------------------------------------------------------


class MaxArraySizeClampTests(unittest.TestCase):
    def test_throttle_above_max_array_size_is_clamped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            # Build enough tasks and a small gpus_per_task so throttle would
            # naturally be huge, then pass a tiny MaxArraySize.
            runs: list[PlannedRun] = []
            for i in range(20):
                runs.append(
                    _planned_run(
                        run_index=i + 1,
                        run_id=f"r{i+1}",
                        model_name="m",
                        cluster_overrides={"gpus_per_node": 1},
                        allocation=AllocationRequest(nodes=1, gpus_per_node=1, cpus_per_task=4, mem="0"),
                        recommended_total_gpus=1,
                        batch_root=batch_root,
                    )
                )
            plan = plan_gpu_budget(
                runs,
                max_available_gpus=1000,
                dispatch_cfg=DispatchConfig(),
                slurm_max_array_size=5,
            )
            self.assertLessEqual(max(g.throttle for g in plan.groups), 5)
            codes = {w.code for w in plan.warnings}
            self.assertIn("throttle_clamped_by_max_array_size", codes)

    def test_default_max_array_size_is_exposed(self) -> None:
        self.assertGreater(DEFAULT_SLURM_MAX_ARRAY_SIZE, 0)


# ---------------------------------------------------------------------------
# Serial-1-group degenerate case: falls back to shared_budget
# ---------------------------------------------------------------------------


class SerialDegenerateTests(unittest.TestCase):
    def test_serial_with_single_group_falls_back_to_shared_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = [
                _planned_run(
                    run_index=i + 1,
                    run_id=f"r{i+1}",
                    model_name="m",
                    cluster_overrides={"gpus_per_node": 2},
                    allocation=AllocationRequest(nodes=1, gpus_per_node=2, cpus_per_task=4, mem="0"),
                    recommended_total_gpus=2,
                    batch_root=batch_root,
                )
                for i in range(4)
            ]
            plan = _build_budget(runs, max_available_gpus=8, policy="serial")
            self.assertEqual(plan.policy_applied, "shared_budget")


# ---------------------------------------------------------------------------
# Config normalization: dispatch enum validation
# ---------------------------------------------------------------------------


class DispatchConfigNormalizationTests(unittest.TestCase):
    def test_dispatch_default_is_error(self) -> None:
        self.assertEqual(normalize_dispatch({}).group_overflow_policy, "error")

    def test_dispatch_accepts_three_policies(self) -> None:
        for policy in ("error", "serial", "best_effort"):
            with self.subTest(policy=policy):
                self.assertEqual(
                    normalize_dispatch({"group_overflow_policy": policy}).group_overflow_policy,
                    policy,
                )

    def test_dispatch_rejects_unknown_policy(self) -> None:
        with self.assertRaises(ConfigContractError):
            normalize_dispatch({"group_overflow_policy": "yolo"})


# ---------------------------------------------------------------------------
# Round-trip: serialize_gpu_budget_plan is a dict
# ---------------------------------------------------------------------------


class SerializeGpuBudgetPlanTests(unittest.TestCase):
    def test_serialize_returns_dict_with_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            runs = [
                _planned_run(
                    run_index=1,
                    run_id="r1",
                    model_name="m",
                    cluster_overrides={"gpus_per_node": 2},
                    allocation=AllocationRequest(nodes=1, gpus_per_node=2, cpus_per_task=4, mem="0"),
                    recommended_total_gpus=2,
                    batch_root=batch_root,
                ),
            ]
            plan = _build_budget(runs, max_available_gpus=8)
            payload = serialize_gpu_budget_plan(plan)
            self.assertIsInstance(payload, dict)
            self.assertEqual(payload["max_available_gpus"], 8)
            self.assertIsInstance(payload["groups"], list)
            self.assertIsInstance(payload["warnings"], list)


# ---------------------------------------------------------------------------
# Validate-surface: iter_compile_report_lines must render the budget plan
# summary and surface plan.warnings via the diagnostic channel.
# ---------------------------------------------------------------------------


def _report_with_plan(plan, extra_batch_diagnostics=()) -> BatchCompileReport:
    """Build a minimal BatchCompileReport whose diagnostics pipeline
    mirrors what build_materialized_report produces: plan.warnings are
    already routed into batch_diagnostics."""
    batch_diagnostics = tuple(extra_batch_diagnostics) + tuple(plan.warnings if plan else ())
    return BatchCompileReport(
        identity=None,
        successful_runs=(),
        failed_runs=(),
        batch_diagnostics=batch_diagnostics,
        checked_runs=0,
        max_available_gpus=(plan.max_available_gpus if plan else 8),
        dispatch_cfg=DispatchConfig(group_overflow_policy=(plan.group_overflow_policy if plan else "error")),
        gpu_budget_plan=plan,
    )


class ValidateReportLinesTests(unittest.TestCase):
    def _make_runs(self, batch_root: Path, gpus_per_node: int, task_count: int, *, group_tag: str) -> list[PlannedRun]:
        runs = [
            _planned_run(
                run_index=i + 1,
                run_id=f"{group_tag}_r{i+1}",
                model_name="m",
                cluster_overrides={"gpus_per_node": gpus_per_node, "partition": f"gpu_{group_tag}"},
                allocation=AllocationRequest(nodes=1, gpus_per_node=gpus_per_node, cpus_per_task=4, mem="0"),
                recommended_total_gpus=gpus_per_node,
                batch_root=batch_root,
            )
            for i in range(task_count)
        ]
        return runs

    def test_summary_line_emitted_for_shared_budget_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "runs" / "demo" / "exp" / "batch_b1"
            runs = _finalize_total_runs(self._make_runs(batch_root, gpus_per_node=2, task_count=4, group_tag="a"))
            plan = _build_budget(runs, max_available_gpus=16)
            report = _report_with_plan(plan)
            out = "\n".join(iter_compile_report_lines(report))
            self.assertIn("GPU batch budget:", out)
            self.assertIn("resources.max_available_gpus=16", out)
            self.assertIn("array_groups=1", out)
            self.assertIn("policy_applied=shared_budget", out)
            self.assertIn("strict_global_limit=true", out)
            # No constrained group, strict_global_limit=true → INFO tone
            self.assertIn("[INFO] GPU batch budget:", out)

    def test_best_effort_warning_surfaces_and_escalates_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "runs" / "demo" / "exp" / "batch_b1"
            runs = _finalize_total_runs([
                *self._make_runs(batch_root, gpus_per_node=8, task_count=1, group_tag="a"),
                *self._make_runs(batch_root, gpus_per_node=8, task_count=1, group_tag="b"),
                *self._make_runs(batch_root, gpus_per_node=8, task_count=1, group_tag="c"),
            ])
            # Re-stamp run_index 1..3 after _finalize
            plan = _build_budget(runs, max_available_gpus=16, policy="best_effort")
            report = _report_with_plan(plan)
            out = "\n".join(iter_compile_report_lines(report))
            self.assertIn("best_effort does not guarantee a strict global GPU limit", out)
            self.assertIn("policy_applied=best_effort", out)
            self.assertIn("strict_global_limit=false", out)
            # best_effort → WARN tone on the summary header
            self.assertIn("[WARN] GPU batch budget:", out)

    def test_serial_summary_annotates_will_serialize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "runs" / "demo" / "exp" / "batch_b1"
            runs = _finalize_total_runs([
                *self._make_runs(batch_root, gpus_per_node=8, task_count=1, group_tag="a"),
                *self._make_runs(batch_root, gpus_per_node=8, task_count=1, group_tag="b"),
                *self._make_runs(batch_root, gpus_per_node=8, task_count=1, group_tag="c"),
            ])
            plan = _build_budget(runs, max_available_gpus=16, policy="serial")
            report = _report_with_plan(plan)
            out = "\n".join(iter_compile_report_lines(report))
            self.assertIn("policy_applied=serialized_groups", out)
            self.assertIn("generate will serialize array groups.", out)

    def test_max_array_size_clamp_warning_surfaces_via_validate_channel(self) -> None:
        # Use the lower-level plan_gpu_budget directly so we can force clamp.
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "runs" / "demo" / "exp" / "batch_b1"
            runs = _finalize_total_runs([
                _planned_run(
                    run_index=i + 1,
                    run_id=f"r{i+1}",
                    model_name="m",
                    cluster_overrides={"gpus_per_node": 1},
                    allocation=AllocationRequest(nodes=1, gpus_per_node=1, cpus_per_task=4, mem="0"),
                    recommended_total_gpus=1,
                    batch_root=batch_root,
                )
                for i in range(20)
            ])
            plan = plan_gpu_budget(
                runs,
                max_available_gpus=1000,
                dispatch_cfg=DispatchConfig(),
                slurm_max_array_size=5,
            )
            report = _report_with_plan(plan)
            out = "\n".join(iter_compile_report_lines(report))
            self.assertIn("throttle_clamped_by_max_array_size", out)

    def test_warning_count_includes_batch_level_budget_warnings(self) -> None:
        """report_warning_count must count budget warnings routed into
        batch_diagnostics, not just per-run stage warnings."""
        fake_warning = PlanDiagnostic(
            severity="warning",
            category="resource",
            code="best_effort_no_strict_global_limit",
            message="synthetic",
            stage="batch",
        )
        report = BatchCompileReport(
            identity=None,
            successful_runs=(),
            failed_runs=(),
            batch_diagnostics=(fake_warning,),
            checked_runs=0,
        )
        self.assertEqual(report_warning_count(report), 1)


# ---------------------------------------------------------------------------
# Compile-source integration: authoring flow must keep batch-scoped and
# run-scoped resource fields separate all the way through planning.
# ---------------------------------------------------------------------------


def _authoring_compile_cfg(
    tmp_path: Path,
    *,
    resources: dict | None = None,
    cluster: dict | None = None,
    dispatch: dict | None = None,
    sweep: dict | None = None,
) -> dict:
    cfg = {
        "project": "demo",
        "experiment_name": "exp",
        "model": {"name": "custom", "script": "train.py", "ddp_supported": True},
        "run": {"args": {}},
        "cluster": {
            "partition": "gpu",
            "account": "acct",
            "qos": "normal",
            "time_limit": "01:00:00",
            "nodes": 1,
            "gpus_per_node": "auto",
            "cpus_per_task": 4,
            "mem": "0",
        },
        "resources": {
            "auto_gpu": False,
            "max_available_gpus": 16,
            "max_gpus_per_job": 8,
        },
    }
    if resources:
        cfg["resources"] = {**cfg["resources"], **resources}
    if cluster:
        cfg["cluster"] = {**cfg["cluster"], **cluster}
    if dispatch:
        cfg["dispatch"] = dict(dispatch)
    if sweep:
        cfg["sweep"] = dict(sweep)
    return cfg


def _compile_authoring_cfg(tmp_path: Path, cfg: dict) -> BatchCompileReport:
    config_path = tmp_path / "experiment.yaml"
    config_path.write_text(json.dumps(cfg, sort_keys=True), encoding="utf-8")
    (tmp_path / "train.py").write_text(
        "import argparse\nparser = argparse.ArgumentParser()\nparser.parse_args()\n",
        encoding="utf-8",
    )
    return compile_source(
        AuthoringSourceRequest(
            config_path=config_path,
            project_root=tmp_path,
            default_batch_name="batch_scope",
        )
    )


class AuthoringBatchScopeIntegrationTests(unittest.TestCase):
    def test_sweep_max_gpus_per_job_is_run_scoped_and_max_available_stays_batch_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            report = _compile_authoring_cfg(
                tmp_path,
                _authoring_compile_cfg(
                    tmp_path,
                    sweep={
                        "enabled": True,
                        "shared_axes": {
                            "resources.max_gpus_per_job": [2, 4],
                        },
                    },
                ),
            )
            planned_batch = require_success(report)

        self.assertEqual(planned_batch.max_available_gpus, 16)
        self.assertEqual(planned_batch.gpu_budget_plan.max_available_gpus, 16)
        self.assertEqual(
            sorted(run.plan.train_stage.max_gpus_per_job for run in planned_batch.planned_runs),
            [2, 4],
        )
        self.assertEqual(
            sorted(
                run.snapshot.replay_spec.replay_cfg["resources"]["max_gpus_per_job"]
                for run in planned_batch.planned_runs
            ),
            [2, 4],
        )

    def test_sweep_auto_gpu_is_allowed_as_run_scoped_resource_axis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            report = _compile_authoring_cfg(
                tmp_path,
                _authoring_compile_cfg(
                    tmp_path,
                    sweep={
                        "enabled": True,
                        "shared_axes": {
                            "resources.auto_gpu": [True, False],
                        },
                    },
                ),
            )
            planned_batch = require_success(report)

        self.assertEqual(planned_batch.max_available_gpus, 16)
        self.assertEqual(
            sorted(
                run.snapshot.replay_spec.replay_cfg["resources"]["auto_gpu"]
                for run in planned_batch.planned_runs
            ),
            [False, True],
        )

    def test_max_gpus_per_job_above_max_available_is_not_capped_but_budget_overflow_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            report = _compile_authoring_cfg(
                tmp_path,
                _authoring_compile_cfg(
                    tmp_path,
                    resources={"max_available_gpus": 4, "max_gpus_per_job": 8},
                    cluster={"gpus_per_node": 8},
                ),
            )

        self.assertTrue(report_has_failures(report))
        self.assertEqual(report.failed_runs, ())
        self.assertEqual([run.plan.train_stage.max_gpus_per_job for run in report.successful_runs], [])
        codes = {diagnostic.code for diagnostic in report.batch_diagnostics}
        self.assertIn("gpu_budget_overflow", codes)
        messages = "\n".join(diagnostic.message for diagnostic in report.batch_diagnostics)
        self.assertIn("exceeds resources.max_available_gpus", messages)


# ---------------------------------------------------------------------------
# Scope-boundary tests: resources.max_available_gpus vs the rest of resources.
# These are the critical tests for the batch-scoped / run-scoped split.
# ---------------------------------------------------------------------------


import inspect  # noqa: E402 — imported near the tests that use it

from slurmforge.pipeline.compiler.batch_scope import resolve_batch_scope_unique  # noqa: E402


class BatchScopeResolverTests(unittest.TestCase):
    def test_unique_candidates_collapse_to_single_value(self) -> None:
        self.assertEqual(
            resolve_batch_scope_unique((8, 8, 8), field_path="resources.max_available_gpus"),
            8,
        )

    def test_divergent_candidates_raise_with_actionable_message(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            resolve_batch_scope_unique((8, 16), field_path="resources.max_available_gpus")
        message = str(ctx.exception)
        self.assertIn("resources.max_available_gpus", message)
        self.assertIn("--set resources.max_available_gpus=", message)
        # Both divergent values visible so the user can decide which one to keep.
        self.assertIn("8", message)
        self.assertIn("16", message)


class PlanGpuBudgetSignatureTests(unittest.TestCase):
    """Enforce the contract that the budget planner sees only batch-scoped inputs.

    Historically the planner accepted a full ``ResourcesConfig``, which
    leaked per-run knobs (``max_gpus_per_job``, estimator profile, ...)
    across the batch.  After the scope split this must not come back.
    """

    def test_signature_accepts_max_available_gpus_int_not_resources_cfg(self) -> None:
        sig = inspect.signature(plan_gpu_budget)
        self.assertIn("max_available_gpus", sig.parameters)
        self.assertNotIn("resources_cfg", sig.parameters)
        param = sig.parameters["max_available_gpus"]
        # Keyword-only (we defined it after ``*``) and non-str annotation.
        self.assertEqual(param.kind, inspect.Parameter.KEYWORD_ONLY)


class SweepBatchScopedFieldRejectionTests(unittest.TestCase):
    """The field contract registry must forbid sweeping batch-scoped fields
    without touching their run-scoped siblings.  Scope decisions flow from
    ``pipeline/config/contracts/fields.py`` — not from hand-written constant
    lists.  ``tests/test_contracts.py`` has the broader coverage; this class
    spot-checks the resources-block boundary specifically."""

    def _validate_paths(
        self,
        *,
        shared_axes=(),
        cases_set=(),
    ) -> None:
        """Exercise validate_batch_scoped_sweep_paths against a hand-built SweepSpec.

        We bypass ``normalize_authoring_sweep_spec`` (which also enforces
        full-config completeness) because these tests scope strictly to the
        sweep-rule predicate.
        """
        from slurmforge.pipeline.config.validation.sweep_rules import validate_batch_scoped_sweep_paths
        from slurmforge.sweep.models import SweepCaseSpec, SweepSpec

        cases = tuple(
            SweepCaseSpec(name=name, set_values=((path, value),), axes=())
            for name, (path, value) in cases_set
        )
        spec = SweepSpec(
            enabled=True,
            shared_axes=tuple(shared_axes),
            cases=cases,
        )
        validate_batch_scoped_sweep_paths(spec, config_path=Path("/tmp/x.yaml"))

    def test_shared_axes_max_available_gpus_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[("resources.max_available_gpus", (8, 16))])
        self.assertIn("resources.max_available_gpus", str(ctx.exception))

    def test_shared_axes_max_gpus_per_job_allowed(self) -> None:
        # No exception — run-scoped resource fields may be swept freely.
        self._validate_paths(shared_axes=[("resources.max_gpus_per_job", (2, 4, 8))])

    def test_shared_axes_auto_gpu_allowed(self) -> None:
        # Other run-scoped resource knobs (auto_gpu, estimator fields) are
        # also permitted as sweep axes.
        self._validate_paths(shared_axes=[("resources.auto_gpu", (True, False))])

    def test_shared_axes_min_gpus_per_job_allowed(self) -> None:
        self._validate_paths(shared_axes=[("resources.min_gpus_per_job", (1, 2))])

    def test_cases_set_dispatch_policy_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(cases_set=[("a", ("dispatch.group_overflow_policy", "serial"))])
        self.assertIn("dispatch.group_overflow_policy", str(ctx.exception))

    def test_mixed_axes_only_batch_scoped_field_is_named_in_error(self) -> None:
        """Corner case: sweeping both a batch-scoped and a run-scoped field
        rejects only because of the batch-scoped one, so the error must name
        ``resources.max_available_gpus`` and NOT accidentally blame
        ``resources.max_gpus_per_job``."""
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[
                ("resources.max_available_gpus", (8, 16)),
                ("resources.max_gpus_per_job", (2, 4)),
            ])
        message = str(ctx.exception)
        self.assertIn("resources.max_available_gpus", message)
        # The allowed axis must not be listed as a violation.
        self.assertNotIn("sweep.shared_axes.resources.max_gpus_per_job", message)


class CompileReportBatchScopeResolutionTests(unittest.TestCase):
    """build_materialized_report must resolve candidates into a single
    max_available_gpus / dispatch_cfg, or error cleanly when runs disagree."""

    def _build(self, *, max_available_gpus_candidates, dispatch_policy_candidates):
        from slurmforge.pipeline.compiler.reports.builders import build_materialized_report
        from slurmforge.pipeline.compiler.state import MaterializedSourceBundle
        from slurmforge.pipeline.compiler.reports.models import SourceCollectionReport
        from slurmforge.pipeline.compiler.requests import AuthoringSourceRequest
        from slurmforge.pipeline.sources.models import SourceInputBatch

        # Minimal materialized bundle sufficient to exercise the report builder.
        source_report = SourceCollectionReport(
            request=AuthoringSourceRequest(config_path=Path("/tmp/x.yaml")),
            batch=SourceInputBatch(
                source_inputs=(),
                checked_inputs=0,
                manifest_extras={},
                failed_runs=(),
                batch_diagnostics=(),
                source_summary="",
            ),
        )
        materialized = MaterializedSourceBundle(
            report=source_report,
            context=None,
            batch_diagnostics=(),
            manifest_extras={},
        )
        return build_materialized_report(
            materialized=materialized,
            identity=None,
            successful_runs=(),
            failed_runs=(),
            checked_runs=0,
            notify_cfg=None,
            submit_dependencies=None,
            max_available_gpus_candidates=max_available_gpus_candidates,
            dispatch_policy_candidates=dispatch_policy_candidates,
        )

    def test_uniform_candidates_produce_single_batch_value(self) -> None:
        report = self._build(
            max_available_gpus_candidates=(8, 8, 8),
            dispatch_policy_candidates=("error", "error", "error"),
        )
        self.assertEqual(report.max_available_gpus, 8)
        self.assertEqual(report.dispatch_cfg.group_overflow_policy, "error")

    def test_divergent_max_available_gpus_becomes_batch_error_diagnostic(self) -> None:
        report = self._build(
            max_available_gpus_candidates=(8, 16),
            dispatch_policy_candidates=("error", "error"),
        )
        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_max_available_gpus", codes)

    def test_divergent_dispatch_policy_becomes_batch_error_diagnostic(self) -> None:
        report = self._build(
            max_available_gpus_candidates=(8, 8),
            dispatch_policy_candidates=("error", "serial"),
        )
        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_dispatch_policy", codes)


if __name__ == "__main__":
    unittest.main()
