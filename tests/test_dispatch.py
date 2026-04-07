from __future__ import annotations

import errno
import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from slurmforge.pipeline.config.normalize import normalize_env, normalize_notify
from slurmforge.pipeline.materialization import materialize_batch
from slurmforge.pipeline.planning import BatchIdentity, PlannedBatch, PlannedRun
from slurmforge.pipeline.planning.contracts import AllocationRequest, ExecutionTopology
from slurmforge.pipeline.records import DispatchInfo, serialize_run_plan
from tests._support import (
    make_template_env,
    sample_run_plan,
    sample_run_snapshot,
    sample_stage_plan,
)


def _planned_run(**plan_overrides) -> PlannedRun:
    plan = sample_run_plan(**plan_overrides)
    snapshot = sample_run_snapshot(
        run_index=plan.run_index,
        total_runs=plan.total_runs,
        run_id=plan.run_id,
        project=plan.project,
        experiment_name=plan.experiment_name,
        model_name=plan.model_name,
        train_mode=plan.train_mode,
    )
    return PlannedRun(plan=plan, snapshot=snapshot)


def _planned_batch(*planned_runs: PlannedRun, batch_root: Path, notify_cfg=None, submit_dependencies=None, manifest_extras=None) -> PlannedBatch:
    batch_name = batch_root.name
    if batch_name.startswith("batch_"):
        batch_name = batch_name[len("batch_") :]
    base_output_dir = batch_root.parent.parent.parent
    return PlannedBatch(
        identity=BatchIdentity(
            project_root=base_output_dir.parent,
            base_output_dir=base_output_dir,
            project=planned_runs[0].plan.project,
            experiment_name=planned_runs[0].plan.experiment_name,
            batch_name=batch_name,
        ),
        planned_runs=tuple(planned_runs),
        notify_cfg=notify_cfg,
        submit_dependencies={} if submit_dependencies is None else submit_dependencies,
        manifest_extras={} if manifest_extras is None else manifest_extras,
    )


class DispatchTests(unittest.TestCase):
    def test_serialize_run_plan_includes_dispatch(self) -> None:
        base_stage = sample_stage_plan()
        plan = sample_run_plan(
            run_id="abc123",
            train_stage=replace(
                base_stage,
                launcher_kind="ddp",
                topology=ExecutionTopology(nodes=1, processes_per_node=2, master_port=29500),
                allocation=AllocationRequest(nodes=1, gpus_per_node=2, cpus_per_task=2, mem="0"),
                estimate=replace(base_stage.estimate, recommended_total_gpus=2, max_useful_total_gpus=2),
            ),
            dispatch=DispatchInfo(
                sbatch_path="/tmp/a.sbatch.sh",
                record_path="/tmp/task.json",
                array_group=1,
                array_task_index=2,
            ),
        )
        payload = serialize_run_plan(plan)
        self.assertIn("dispatch", payload)
        self.assertIn("generated_by", payload)
        self.assertIn("train_stage", payload)
        self.assertIn("planning_diagnostics", payload)
        self.assertEqual(payload["dispatch"]["array_task_index"], 2)

    def test_array_dispatch_streams_records_and_uses_zero_based_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            planned_runs = [
                _planned_run(
                    run_index=1,
                    total_runs=2,
                    run_id="r1",
                    run_dir=str(batch_root / "runs" / "run_001_r1"),
                    run_dir_rel="runs/run_001_r1",
                    train_stage=sample_stage_plan(workdir=tmp_path),
                ),
                _planned_run(
                    run_index=2,
                    total_runs=2,
                    run_id="r2",
                    run_dir=str(batch_root / "runs" / "run_002_r2"),
                    run_dir_rel="runs/run_002_r2",
                    train_stage=sample_stage_plan(workdir=tmp_path),
                ),
            ]

            result = materialize_batch(
                planned_batch=_planned_batch(*planned_runs, batch_root=batch_root),
                env=make_template_env(),
            )

            self.assertEqual(len(result.array_groups_meta), 1)
            meta = result.array_groups_meta[0]
            self.assertEqual(meta["array_size"], 2)
            self.assertTrue(batch_root.exists())
            self.assertEqual(list(tmp_path.glob(".batch.staging-*")), [])

            records_dir = Path(meta["records_dir"])
            rec0 = records_dir / "task_000000.json"
            rec1 = records_dir / "task_000001.json"
            self.assertTrue(rec0.exists())
            self.assertTrue(rec1.exists())
            self.assertEqual(json.loads(rec0.read_text())["run_index"], 1)
            self.assertEqual(json.loads(rec1.read_text())["run_index"], 2)
            self.assertIn("group_reason", meta)
            self.assertIn("identical Slurm resources (", meta["group_reason"])
            self.assertIn("runtime environment bootstrap (", meta["group_reason"])
            self.assertIn("resource_request", meta)
            self.assertEqual(meta["resource_request"]["gpus_per_node"], 1)
            self.assertIn("runtime_env", meta)
            record_payload = json.loads(rec0.read_text())
            self.assertEqual(record_payload["generated_by"]["name"], "slurmforge")
            self.assertEqual(record_payload["dispatch"]["array_assignment"]["group_index"], 1)
            self.assertIn("group_reason", record_payload["dispatch"]["array_assignment"])
            self.assertIsNone(planned_runs[0].plan.dispatch.array_group)
            self.assertIsNone(planned_runs[0].plan.dispatch.array_task_index)
            self.assertEqual(planned_runs[0].plan.dispatch.sbatch_path, "")
            self.assertIsNone(planned_runs[0].plan.dispatch.record_path)

            snapshot_path = batch_root / "runs" / "run_001_r1" / "meta" / "run_snapshot.json"
            self.assertTrue(snapshot_path.exists())

            sbatch_text = Path(meta["sbatch_path"]).read_text()
            self.assertIn("#SBATCH --array=0-1", sbatch_text)
            self.assertIn("sforge-run-plan-executor", sbatch_text)
            self.assertNotIn("-m slurmforge.execution.run_plan_executor", sbatch_text)
            submit_text = result.submit_script.read_text()
            self.assertIn("[BATCH] array_groups=1", submit_text)
            self.assertIn("sbatch --parsable", submit_text)
            self.assertIn("[SUBMITTED] group=1 job_id=${JOB_ID}", submit_text)

            manifest = json.loads(result.manifest_path.read_text())
            self.assertEqual(manifest["generated_by"]["name"], "slurmforge")
            self.assertEqual(manifest["array_group_count"], 1)
            self.assertEqual(len(manifest["resource_buckets"]), 1)
            self.assertEqual(manifest["resource_buckets"][0]["resource_request"]["gpus_per_node"], 1)

    def test_materialize_batch_adds_batch_completion_notify_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            planned_run = _planned_run(
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                run_dir_rel="runs/run_001_r1",
                train_stage=sample_stage_plan(workdir=tmp_path),
            )

            result = materialize_batch(
                planned_batch=_planned_batch(
                    planned_run,
                    batch_root=batch_root,
                    notify_cfg=normalize_notify({"enabled": True, "email": "you@example.com", "when": "afterany"}),
                ),
                env=make_template_env(),
            )

            submit_text = result.submit_script.read_text()
            self.assertIn("JOB_IDS=()", submit_text)
            self.assertIn('JOB_IDS+=("${JOB_ID%%;*}")', submit_text)
            self.assertIn("--dependency=afterany:${DEPENDENCY_IDS}", submit_text)
            self.assertIn("--mail-user=you@example.com", submit_text)
            self.assertIn("--mail-type=END", submit_text)

    def test_materialize_batch_splits_array_groups_when_runtime_environment_differs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            planned_runs = [
                _planned_run(
                    run_index=1,
                    total_runs=2,
                    run_id="r1",
                    run_dir=str(batch_root / "runs" / "run_001_r1"),
                    run_dir_rel="runs/run_001_r1",
                    train_stage=sample_stage_plan(workdir=tmp_path),
                    env=normalize_env({"venv_activate": "source /shared/env_a/bin/activate"}),
                ),
                _planned_run(
                    run_index=2,
                    total_runs=2,
                    run_id="r2",
                    run_dir=str(batch_root / "runs" / "run_002_r2"),
                    run_dir_rel="runs/run_002_r2",
                    train_stage=sample_stage_plan(workdir=tmp_path),
                    env=normalize_env({"venv_activate": "source /shared/env_b/bin/activate"}),
                ),
            ]

            result = materialize_batch(
                planned_batch=_planned_batch(*planned_runs, batch_root=batch_root),
                env=make_template_env(),
            )

            self.assertEqual(len(result.array_groups_meta), 2)
            runtime_envs = [meta["runtime_env"]["venv_activate"] for meta in result.array_groups_meta]
            self.assertIn("source /shared/env_a/bin/activate", runtime_envs)
            self.assertIn("source /shared/env_b/bin/activate", runtime_envs)

    def test_materialize_batch_applies_external_submit_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_deps"
            planned_run = _planned_run(
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                run_dir_rel="runs/run_001_r1",
                train_stage=sample_stage_plan(workdir=tmp_path),
            )
            result = materialize_batch(
                planned_batch=_planned_batch(
                    planned_run,
                    batch_root=batch_root,
                    submit_dependencies={"afterok": ["101", "202"], "afterany": ["303"]},
                ),
                env=make_template_env(),
            )

            submit_text = result.submit_script.read_text(encoding="utf-8")
            self.assertIn("--dependency=afterany:303,afterok:101:202", submit_text)

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["submit_dependencies"],
                {"afterany": ["303"], "afterok": ["101", "202"]},
            )

    def test_materialize_batch_falls_back_to_shutil_move_on_exdev(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "runs" / "demo" / "exp" / "batch_b1"
            planned_run = _planned_run(
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                run_dir_rel="runs/run_001_r1",
                train_stage=sample_stage_plan(workdir=tmp_path),
            )

            with patch(
                "slurmforge.pipeline.materialization.commit.Path.rename",
                side_effect=OSError(errno.EXDEV, "Invalid cross-device link"),
            ):
                with patch(
                    "slurmforge.pipeline.materialization.commit.shutil.move",
                    wraps=shutil.move,
                ) as move_mock:
                    result = materialize_batch(
                        planned_batch=_planned_batch(planned_run, batch_root=batch_root),
                        env=make_template_env(),
                    )
            self.assertTrue(move_mock.called)
            self.assertTrue(result.manifest_path.exists())
