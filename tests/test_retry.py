from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from slurmforge.pipeline.compiler import BatchCompileError, RetrySourceRequest, compile_source
from slurmforge.pipeline.compiler.reports import report_has_failures, require_success
from slurmforge.pipeline.config.api import EvalTrainOutputsConfig
from slurmforge.pipeline.config.codecs import normalize_storage_config
from slurmforge.pipeline.materialization import materialize_batch
from slurmforge.pipeline.planning import BatchIdentity, PlannedBatch, PlannedRun
from tests._support import sample_run_plan, sample_run_snapshot, sample_stage_plan, write_test_descriptor

from slurmforge.pipeline.sources.replay import collect_retry_source_inputs
from slurmforge.pipeline.records import build_replay_spec, serialize_run_plan, serialize_run_snapshot
from slurmforge.pipeline.status import (
    ExecutionStatus,
    serialize_execution_status,
    status_path_for_result_dir,
    write_latest_result_dir,
)
from slurmforge.pipeline.train_outputs import write_train_outputs_contract


def _compile_retry_report(
    *,
    source_batch_root: Path,
    project_root_override: Path | None,
    status_query: str,
    cli_overrides: list[str],
    default_batch_name: str,
):
    return compile_source(
        RetrySourceRequest(
            source_batch_root=source_batch_root,
            project_root=project_root_override,
            status_query=status_query,
            cli_overrides=tuple(cli_overrides),
            default_batch_name=default_batch_name,
        )
    )


def _compile_retry_planned_batch(
    *,
    source_batch_root: Path,
    project_root_override: Path | None,
    status_query: str,
    cli_overrides: list[str],
    default_batch_name: str,
):
    return require_success(
        _compile_retry_report(
            source_batch_root=source_batch_root,
            project_root_override=project_root_override,
            status_query=status_query,
            cli_overrides=cli_overrides,
            default_batch_name=default_batch_name,
        )
    )


def _resolved_cfg(
    *,
    lr: float = 0.001,
    max_available_gpus: int = 2,
    max_gpus_per_job: int = 2,
    gpus_per_node: int = 1,
    dispatch_policy: str = "error",
) -> dict:
    return {
        "project": "demo",
        "experiment_name": "exp",
        "resolved_model_catalog": {"models": []},
        "run": {
            "command": "python3 train.py",
            "args": {"lr": lr},
        },
        "cluster": {
            "partition": "gpu",
            "account": "proj",
            "qos": "normal",
            "time_limit": "01:00:00",
            "nodes": 1,
            "gpus_per_node": gpus_per_node,
            "cpus_per_task": 2,
            "mem": "0",
        },
        "resources": {
            "auto_gpu": False,
            "max_available_gpus": max_available_gpus,
            "max_gpus_per_job": max_gpus_per_job,
        },
        "dispatch": {"group_overflow_policy": dispatch_policy},
    }


def _write_status(run_dir: Path, job_id: str, status: ExecutionStatus) -> None:
    result_dir = run_dir / f"job-{job_id}"
    path = status_path_for_result_dir(result_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(serialize_execution_status(status), indent=2, sort_keys=True), encoding="utf-8")
    write_latest_result_dir(run_dir, result_dir)


def _materialize_retry_batch(
    tmp_path: Path,
    *,
    storage_cfg_dict: dict | None = None,
) -> tuple[Path, Path, Path]:
    from slurmforge.storage import create_planning_store
    from tests._support import make_template_env

    storage_config = normalize_storage_config(storage_cfg_dict)
    identity = BatchIdentity(
        project_root=tmp_path,
        base_output_dir=tmp_path / "runs",
        project="demo",
        experiment_name="exp",
        batch_name="retry_src",
    )
    batch_root = identity.batch_root
    run_dir_1 = batch_root / "runs" / "run_001_r1"
    run_dir_2 = batch_root / "runs" / "run_002_r2"

    plan1 = sample_run_plan(
        run_index=1,
        total_runs=2,
        run_id="r1",
        run_dir=str(run_dir_1),
        run_dir_rel="runs/run_001_r1",
        train_stage=sample_stage_plan(workdir=tmp_path),
    )
    plan2 = sample_run_plan(
        run_index=2,
        total_runs=2,
        run_id="r2",
        run_dir=str(run_dir_2),
        run_dir_rel="runs/run_002_r2",
        train_stage=sample_stage_plan(workdir=tmp_path),
    )
    snap1 = sample_run_snapshot(
        run_index=1,
        total_runs=2,
        run_id="r1",
        replay_spec=build_replay_spec(
            _resolved_cfg(lr=0.001),
            planning_root=str(tmp_path),
            source_batch_root=str(batch_root),
            source_run_id="r1",
            source_record_path=str(batch_root / "records" / "group_01" / "task_000000.json"),
        ),
    )
    snap2 = sample_run_snapshot(
        run_index=2,
        total_runs=2,
        run_id="r2",
        replay_spec=build_replay_spec(
            _resolved_cfg(lr=0.002),
            planning_root=str(tmp_path),
            source_batch_root=str(batch_root),
            source_run_id="r2",
            source_record_path=str(batch_root / "records" / "group_01" / "task_000001.json"),
        ),
    )

    planned_batch = PlannedBatch(
        identity=identity,
        planned_runs=(PlannedRun(plan=plan1, snapshot=snap1), PlannedRun(plan=plan2, snapshot=snap2)),
        storage_config=storage_config,
    )
    store = create_planning_store(storage_config, make_template_env())
    materialize_batch(planned_batch=planned_batch, planning_store=store)
    return batch_root, run_dir_1, run_dir_2


def _write_snapshot(run_dir: Path, snapshot) -> None:
    snapshot_path = run_dir / "meta" / "run_snapshot.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(serialize_run_snapshot(snapshot), sort_keys=True), encoding="utf-8")


def _materialize_failed_retry_batch_with_cfgs(tmp_path: Path, cfg1: dict, cfg2: dict) -> Path:
    batch_root, run_dir_1, run_dir_2 = _materialize_retry_batch(tmp_path)
    (tmp_path / "train.py").write_text("print('ok')\n", encoding="utf-8")

    for index, run_id, run_dir, cfg, job_id in (
        (1, "r1", run_dir_1, cfg1, "101"),
        (2, "r2", run_dir_2, cfg2, "102"),
    ):
        _write_snapshot(
            run_dir,
            sample_run_snapshot(
                run_index=index,
                total_runs=2,
                run_id=run_id,
                replay_spec=build_replay_spec(
                    cfg,
                    planning_root=str(tmp_path),
                    source_batch_root=str(batch_root),
                    source_run_id=run_id,
                    source_record_path=str(batch_root / "records" / "group_01" / f"task_{index - 1:06d}.json"),
                ),
            ),
        )
        _write_status(
            run_dir,
            job_id,
            ExecutionStatus(
                state="failed",
                failure_class="script_error",
                failed_stage="train",
                reason="train_exit_code=1",
                job_key=job_id,
                slurm_job_id=job_id,
                result_dir=str(run_dir / f"job-{job_id}"),
            ),
        )
    return batch_root


class RetryTests(unittest.TestCase):
    def test_collect_retry_source_inputs_filters_failed_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)

            plan1 = sample_run_plan(
                run_index=1,
                total_runs=2,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
            )
            plan2 = sample_run_plan(
                run_index=2,
                total_runs=2,
                run_id="r2",
                run_dir=str(batch_root / "runs" / "run_002_r2"),
            )
            manifest_path.write_text(
                "\n".join(
                    [
                        json.dumps(serialize_run_plan(plan1), sort_keys=True),
                        json.dumps(serialize_run_plan(plan2), sort_keys=True),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            _write_status(
                Path(plan1.run_dir),
                "101",
                ExecutionStatus(state="success", job_key="101", slurm_job_id="101", result_dir=str(Path(plan1.run_dir) / "job-101")),
            )
            _write_status(
                Path(plan2.run_dir),
                "102",
                ExecutionStatus(
                    state="failed",
                    failure_class="oom",
                    failed_stage="train",
                    reason="matched OOM",
                    job_key="102",
                    slurm_job_id="102",
                    result_dir=str(Path(plan2.run_dir) / "job-102"),
                ),
            )
            _write_snapshot(
                Path(plan2.run_dir),
                sample_run_snapshot(
                    run_index=2,
                    total_runs=2,
                    run_id="r2",
                    replay_spec=build_replay_spec(_resolved_cfg(lr=0.002), planning_root=str(batch_root)),
                ),
            )

            collection = collect_retry_source_inputs(
                source_batch_root=batch_root,
                status_query="failed",
                cli_overrides=[],
            )

        self.assertEqual(collection.checked_inputs, 1)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r2")
        self.assertEqual(collection.manifest_extras["retry_source"]["selected_run_ids"], ["r2"])
        self.assertEqual(collection.manifest_extras["retry_source"]["selected_failure_counts"], {"oom": 1})

    def test_collect_retry_source_inputs_uses_storage_in_sqlite_pure_db_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root, run_dir_1, run_dir_2 = _materialize_retry_batch(
                tmp_path,
                storage_cfg_dict={
                    "backend": {"engine": "sqlite"},
                    "exports": {"planning_recovery": False},
                },
            )

            _write_status(
                run_dir_1,
                "101",
                ExecutionStatus(
                    state="success",
                    job_key="101",
                    slurm_job_id="101",
                    result_dir=str(run_dir_1 / "job-101"),
                ),
            )
            _write_status(
                run_dir_2,
                "102",
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="train_exit_code=2",
                    job_key="102",
                    slurm_job_id="102",
                    result_dir=str(run_dir_2 / "job-102"),
                ),
            )

            collection = collect_retry_source_inputs(
                source_batch_root=batch_root,
                status_query="failed",
                cli_overrides=[],
            )

        self.assertEqual(collection.checked_inputs, 1)
        self.assertEqual(len(collection.failed_runs), 0)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r2")
        # In pure DB mode (planning_recovery=false), record files don't exist.
        self.assertIsNone(collection.source_inputs[0].source.config_path)
        self.assertIsNone(collection.source_inputs[0].source.source_record_path)
        self.assertEqual(collection.manifest_extras["retry_source"]["selected_run_ids"], ["r2"])
        self.assertEqual(collection.manifest_extras["retry_source"]["selected_failure_counts"], {"script_error": 1})
        self.assertFalse((batch_root / "records").exists())
        self.assertFalse((run_dir_2 / "meta" / "run_snapshot.json").exists())

    def test_build_retry_planned_batch_rebuilds_failed_runs_and_applies_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "train.py").write_text(
                "import argparse\nparser = argparse.ArgumentParser()\nparser.add_argument('--lr')\nparser.add_argument('--resume_from_checkpoint')\nparser.parse_args()\n",
                encoding="utf-8",
            )
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)

            source_plan = sample_run_plan(
                run_index=2,
                total_runs=2,
                run_id="r2",
                run_dir=str(batch_root / "runs" / "run_002_r2"),
                train_mode="command",
                model_name="external",
                train_stage=replace(
                    sample_stage_plan(),
                    invocation_kind="external_command",
                    launcher_kind="external",
                    command_mode="argv",
                    requested_launcher_mode="external",
                ),
            )
            manifest_path.write_text(
                json.dumps(serialize_run_plan(source_plan), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            _write_snapshot(
                Path(source_plan.run_dir),
                sample_run_snapshot(
                    run_index=2,
                    total_runs=2,
                    run_id="r2",
                    model_name="external",
                    train_mode="command",
                    sweep_case_name="failed_case",
                    sweep_assignments={"run.args.lr": 0.002},
                    replay_spec=build_replay_spec(
                        _resolved_cfg(lr=0.002),
                        planning_root=str(tmp_path),
                        source_batch_root=str(batch_root),
                        source_run_id="r2",
                        source_record_path=str(batch_root / "records" / "group_01" / "task_000001.json"),
                    ),
                ),
            )
            _write_status(
                Path(source_plan.run_dir),
                "102",
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="train_exit_code=2",
                    job_key="102",
                    slurm_job_id="102",
                    result_dir=str(Path(source_plan.run_dir) / "job-102"),
                ),
            )

            planned_batch = _compile_retry_planned_batch(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=["cluster.mem=128G", "run.args.lr=0.123"],
                default_batch_name="retry_batch",
            )

        planned_runs = planned_batch.planned_runs
        self.assertEqual(len(planned_runs), 1)
        self.assertEqual(planned_runs[0].plan.run_index, 1)
        self.assertEqual(planned_runs[0].plan.total_runs, 1)
        self.assertEqual(planned_runs[0].plan.cluster.mem, "128G")
        self.assertEqual(planned_runs[0].snapshot.replay_spec.replay_cfg["run"]["args"]["lr"], 0.123)
        self.assertEqual(planned_runs[0].plan.run_dir, "")
        self.assertTrue(planned_runs[0].plan.run_dir_rel.endswith(planned_runs[0].plan.run_id))
        self.assertEqual(
            planned_batch.batch_root,
            (tmp_path / "runs" / "demo" / "exp" / "batch_retry_batch").resolve(),
        )
        self.assertEqual(planned_batch.manifest_extras["retry_source"]["status_query"], "failed")
        self.assertEqual(planned_batch.manifest_extras["retry_source"]["planning_root"], str(tmp_path.resolve()))
        self.assertEqual(planned_batch.manifest_extras["retry_source"]["selected_run_indices"], [2])
        self.assertEqual(planned_runs[0].snapshot.replay_spec.source_run_id, "r2")
        self.assertEqual(planned_runs[0].snapshot.replay_spec.source_batch_root, str(batch_root.resolve()))
        self.assertEqual(planned_runs[0].plan.sweep_case_name, "failed_case")
        self.assertEqual(planned_runs[0].snapshot.sweep_case_name, "failed_case")
        self.assertEqual(planned_runs[0].plan.sweep_assignments, {"run.args.lr": 0.002})

    def test_collect_retry_source_inputs_keeps_loading_after_bad_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)
            (tmp_path / "train.py").write_text("print('ok')\n", encoding="utf-8")

            plan1 = sample_run_plan(
                run_index=1,
                total_runs=2,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
            )
            plan2 = sample_run_plan(
                run_index=2,
                total_runs=2,
                run_id="r2",
                run_dir=str(batch_root / "runs" / "run_002_r2"),
            )
            manifest_path.write_text(
                "\n".join(
                    [
                        json.dumps(serialize_run_plan(plan1), sort_keys=True),
                        json.dumps(serialize_run_plan(plan2), sort_keys=True),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            bad_run_dir = Path(plan1.run_dir)
            bad_run_dir.mkdir(parents=True, exist_ok=True)
            _write_status(
                bad_run_dir,
                "101",
                ExecutionStatus(
                    state="failed",
                    failure_class="oom",
                    failed_stage="train",
                    reason="matched OOM",
                    job_key="101",
                    slurm_job_id="101",
                    result_dir=str(bad_run_dir / "job-101"),
                ),
            )
            (bad_run_dir / "meta").mkdir(parents=True, exist_ok=True)
            (bad_run_dir / "meta" / "run_snapshot.json").write_text("{bad-json", encoding="utf-8")

            good_run_dir = Path(plan2.run_dir)
            good_run_dir.mkdir(parents=True, exist_ok=True)
            _write_status(
                good_run_dir,
                "102",
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="train_exit_code=2",
                    job_key="102",
                    slurm_job_id="102",
                    result_dir=str(good_run_dir / "job-102"),
                ),
            )
            _write_snapshot(
                good_run_dir,
                sample_run_snapshot(
                    run_index=2,
                    total_runs=2,
                    run_id="r2",
                    replay_spec=build_replay_spec(
                        {
                            "project": "demo",
                            "experiment_name": "exp",
                            "resolved_model_catalog": {"models": []},
                            "model": {"name": "demo", "script": "train.py"},
                            "run": {"mode": "model_cli", "args": {"lr": 0.002}},
                        },
                        planning_root=str(tmp_path),
                        source_batch_root=str(batch_root),
                        source_run_id="r2",
                        source_record_path=str(batch_root / "records" / "group_01" / "task_000001.json"),
                    ),
                ),
            )

            collection = collect_retry_source_inputs(
                source_batch_root=batch_root,
                status_query="failed",
                cli_overrides=[],
            )

        self.assertEqual(collection.checked_inputs, 2)
        self.assertEqual(len(collection.failed_runs), 1)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r2")
        self.assertEqual(collection.source_inputs[0].source_index, 2)
        self.assertIn("r1", collection.failed_runs[0].diagnostics[0].message)

    def test_build_retry_planned_batch_injects_resume_from_latest_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)
            (tmp_path / "train.py").write_text(
                "\n".join(
                    [
                        "import argparse",
                        "",
                        "parser = argparse.ArgumentParser()",
                        "parser.add_argument('--lr', type=float, required=True)",
                        "parser.add_argument('--resume_from_checkpoint', default='')",
                        "parser.parse_args()",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            source_plan = sample_run_plan(
                run_index=1,
                total_runs=1,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                train_mode="model_cli",
            )
            manifest_path.write_text(json.dumps(serialize_run_plan(source_plan), sort_keys=True) + "\n", encoding="utf-8")
            _write_snapshot(
                Path(source_plan.run_dir),
                sample_run_snapshot(
                    run_index=1,
                    total_runs=1,
                    run_id="r1",
                    model_name="demo",
                    train_mode="model_cli",
                    replay_spec=build_replay_spec(
                        {
                            "project": "demo",
                            "experiment_name": "exp",
                            "resolved_model_catalog": {"models": []},
                            "model": {"name": "demo", "script": "train.py"},
                            "run": {"mode": "model_cli", "args": {"lr": 0.002}},
                        },
                        planning_root=str(tmp_path),
                        source_batch_root=str(batch_root),
                        source_run_id="r1",
                        source_record_path=str(batch_root / "records" / "group_01" / "task_000000.json"),
                    ),
                ),
            )

            result_dir = Path(source_plan.run_dir) / "job-501"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            latest = checkpoint_dir / "latest.ckpt"
            latest.write_text("checkpoint", encoding="utf-8")
            _write_status(
                Path(source_plan.run_dir),
                "501",
                ExecutionStatus(
                    state="failed",
                    failure_class="preempted",
                    failed_stage="train",
                    reason="reported by sacct: PREEMPTED",
                    job_key="501",
                    slurm_job_id="501",
                    result_dir=str(result_dir),
                ),
            )

            planned_batch = _compile_retry_planned_batch(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=[],
                default_batch_name="retry_batch",
            )

        planned_runs = planned_batch.planned_runs
        self.assertEqual(
            planned_runs[0].snapshot.replay_spec.replay_cfg["run"]["resume_from_checkpoint"],
            "batch_src/runs/run_001_r1/job-501/checkpoints/latest.ckpt",
        )
        self.assertEqual(
            planned_runs[0].plan.env.extra_env["AI_INFRA_RESUME_FROM_CHECKPOINT"],
            str(latest.resolve()),
        )
        self.assertEqual(planned_batch.batch_name, "retry_batch")

    def test_build_retry_planned_batch_prefers_train_outputs_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)
            (tmp_path / "train.py").write_text("print('ok')\n", encoding="utf-8")

            source_plan = sample_run_plan(
                run_index=1,
                total_runs=1,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                train_mode="model_cli",
                eval_train_outputs=EvalTrainOutputsConfig(required=True, checkpoint_policy="best"),
            )
            manifest_path.write_text(json.dumps(serialize_run_plan(source_plan), sort_keys=True) + "\n", encoding="utf-8")
            _write_snapshot(
                Path(source_plan.run_dir),
                sample_run_snapshot(
                    run_index=1,
                    total_runs=1,
                    run_id="r1",
                    model_name="demo",
                    train_mode="model_cli",
                    replay_spec=build_replay_spec(
                        {
                            "project": "demo",
                            "experiment_name": "exp",
                            "resolved_model_catalog": {"models": []},
                            "model": {"name": "demo", "script": "train.py"},
                            "run": {"mode": "model_cli", "args": {"lr": 0.002}},
                        },
                        planning_root=str(tmp_path),
                        source_batch_root=str(batch_root),
                        source_run_id="r1",
                        source_record_path=str(batch_root / "records" / "group_01" / "task_000000.json"),
                    ),
                ),
            )

            result_dir = Path(source_plan.run_dir) / "job-777"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            preferred = checkpoint_dir / "best-step-3.ckpt"
            ignored = checkpoint_dir / "step-9.ckpt"
            preferred.write_text("preferred", encoding="utf-8")
            ignored.write_text("ignored", encoding="utf-8")
            write_train_outputs_contract(
                result_dir=result_dir,
                manifest_path=result_dir / "meta" / "train_outputs.json",
                env_path=result_dir / "meta" / "train_outputs.env",
                checkpoint_globs=[],
                run_id=source_plan.run_id,
                model_name=source_plan.model_name,
                primary_policy="best",
            )
            _write_status(
                Path(source_plan.run_dir),
                "777",
                ExecutionStatus(
                    state="failed",
                    failure_class="preempted",
                    failed_stage="train",
                    reason="reported by sacct: PREEMPTED",
                    job_key="777",
                    slurm_job_id="777",
                    result_dir=str(result_dir),
                ),
            )

            planned_batch = _compile_retry_planned_batch(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=[],
                default_batch_name="retry_batch",
            )

        planned_runs = planned_batch.planned_runs
        self.assertEqual(
            planned_runs[0].plan.env.extra_env["AI_INFRA_RESUME_FROM_CHECKPOINT"],
            str(preferred.resolve()),
        )

    def test_collect_retry_source_inputs_uses_current_batch_location_after_move(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            old_batch = tmp_path / "old_batch"
            moved_batch = tmp_path / "moved_batch"
            manifest_path = moved_batch / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(moved_batch)

            source_plan = sample_run_plan(
                run_index=1,
                total_runs=1,
                run_id="r1",
                run_dir=str(old_batch / "runs" / "run_001_r1"),
            )
            manifest_path.write_text(json.dumps(serialize_run_plan(source_plan), sort_keys=True) + "\n", encoding="utf-8")
            current_run_dir = moved_batch / "runs" / "run_001_r1"
            _write_snapshot(
                current_run_dir,
                sample_run_snapshot(
                    run_index=1,
                    total_runs=1,
                    run_id="r1",
                    replay_spec=build_replay_spec(_resolved_cfg(lr=0.111), planning_root=str(tmp_path)),
                ),
            )
            _write_status(
                current_run_dir,
                "201",
                ExecutionStatus(
                    state="failed",
                    failure_class="oom",
                    failed_stage="train",
                    reason="matched OOM",
                    job_key="201",
                    slurm_job_id="201",
                    result_dir=str(current_run_dir / "job-201"),
                ),
            )

            collection = collect_retry_source_inputs(
                source_batch_root=moved_batch,
                status_query="failed",
                cli_overrides=[],
            )

        self.assertEqual(collection.checked_inputs, 1)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_batch_root, moved_batch.resolve())
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r1")
        self.assertEqual(collection.manifest_extras["retry_source"]["selected_failure_counts"], {"oom": 1})

    def test_build_retry_planned_batch_uses_canonical_batch_scope_from_replay_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)

            source_plan = sample_run_plan(
                run_index=1,
                total_runs=1,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
            )
            manifest_path.write_text(json.dumps(serialize_run_plan(source_plan), sort_keys=True) + "\n", encoding="utf-8")
            _write_snapshot(
                Path(source_plan.run_dir),
                sample_run_snapshot(
                    run_index=1,
                    total_runs=1,
                    run_id="r1",
                    replay_spec=build_replay_spec(
                        {
                            "project": "demo_retry",
                            "experiment_name": "exp_retry",
                            "resolved_model_catalog": {"models": []},
                            "model": {"name": "demo", "script": "train.py"},
                            "run": {"mode": "model_cli", "args": {"lr": 0.002}},
                            "output": {
                                "base_output_dir": "./retry_runs",
                                "dependencies": {"afterok": ["12345"]},
                            },
                            "notify": {
                                "enabled": True,
                                "email": "retry@example.com",
                                "when": "afterok",
                            },
                        },
                        planning_root=str(tmp_path),
                    ),
                ),
            )
            _write_status(
                Path(source_plan.run_dir),
                "201",
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="train_exit_code=1",
                    job_key="201",
                    slurm_job_id="201",
                    result_dir=str(Path(source_plan.run_dir) / "job-201"),
                ),
            )
            (tmp_path / "train.py").write_text("print('ok')\n", encoding="utf-8")

            planned_batch = _compile_retry_planned_batch(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=[],
                default_batch_name="retry_default",
            )

        planned_runs = planned_batch.planned_runs
        self.assertEqual(len(planned_runs), 1)
        self.assertEqual(planned_batch.project, "demo_retry")
        self.assertEqual(planned_batch.experiment_name, "exp_retry")
        self.assertEqual(
            planned_batch.batch_root,
            (tmp_path / "retry_runs" / "demo_retry" / "exp_retry" / "batch_retry_default").resolve(),
        )
        self.assertTrue(planned_batch.notify_cfg.enabled)
        self.assertEqual(planned_batch.notify_cfg.email, "retry@example.com")
        self.assertEqual(planned_batch.submit_dependencies, {"afterok": ["12345"]})

    def test_retry_rejects_selected_runs_with_divergent_max_available_gpus_without_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = _materialize_failed_retry_batch_with_cfgs(
                tmp_path,
                _resolved_cfg(lr=0.001, max_available_gpus=8),
                _resolved_cfg(lr=0.002, max_available_gpus=16),
            )
            report = _compile_retry_report(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=[],
                default_batch_name="retry_batch",
            )

        self.assertTrue(report_has_failures(report))
        self.assertEqual(report.successful_runs, ())
        codes = {diagnostic.code for diagnostic in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_max_available_gpus", codes)
        messages = "\n".join(diagnostic.message for diagnostic in report.batch_diagnostics)
        self.assertIn("--set resources.max_available_gpus=", messages)

    def test_retry_cli_override_unifies_divergent_max_available_gpus_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = _materialize_failed_retry_batch_with_cfgs(
                tmp_path,
                _resolved_cfg(lr=0.001, max_available_gpus=8),
                _resolved_cfg(lr=0.002, max_available_gpus=16),
            )
            planned_batch = _compile_retry_planned_batch(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=["resources.max_available_gpus=32"],
                default_batch_name="retry_batch",
            )

        self.assertEqual(planned_batch.max_available_gpus, 32)
        self.assertEqual(planned_batch.gpu_budget_plan.max_available_gpus, 32)
        self.assertEqual(
            {
                run.snapshot.replay_spec.replay_cfg["resources"]["max_available_gpus"]
                for run in planned_batch.planned_runs
            },
            {32},
        )

    def test_retry_cli_override_max_gpus_per_job_remains_run_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = _materialize_failed_retry_batch_with_cfgs(
                tmp_path,
                _resolved_cfg(lr=0.001, max_available_gpus=16, max_gpus_per_job=2),
                _resolved_cfg(lr=0.002, max_available_gpus=16, max_gpus_per_job=2),
            )
            planned_batch = _compile_retry_planned_batch(
                source_batch_root=batch_root,
                project_root_override=tmp_path,
                status_query="failed",
                cli_overrides=["resources.max_gpus_per_job=4"],
                default_batch_name="retry_batch",
            )

        self.assertEqual(planned_batch.max_available_gpus, 16)
        self.assertEqual(
            {run.plan.train_stage.max_gpus_per_job for run in planned_batch.planned_runs},
            {4},
        )
        self.assertEqual(
            {
                run.snapshot.replay_spec.replay_cfg["resources"]["max_gpus_per_job"]
                for run in planned_batch.planned_runs
            },
            {4},
        )

    def test_build_retry_planned_batch_reports_human_readable_replay_source_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)

            source_plan = sample_run_plan(
                run_index=1,
                total_runs=1,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
            )
            manifest_path.write_text(json.dumps(serialize_run_plan(source_plan), sort_keys=True) + "\n", encoding="utf-8")
            _write_snapshot(
                Path(source_plan.run_dir),
                sample_run_snapshot(
                    run_index=1,
                    total_runs=1,
                    run_id="r1",
                    replay_spec=build_replay_spec(
                        {
                            "project": "demo",
                            "experiment_name": "exp",
                            "resolved_model_catalog": {"models": []},
                            "run": "not-a-mapping",
                        },
                        planning_root=str(tmp_path),
                    ),
                ),
            )
            _write_status(
                Path(source_plan.run_dir),
                "201",
                ExecutionStatus(
                    state="failed",
                    failure_class="script_error",
                    failed_stage="train",
                    reason="train_exit_code=1",
                    job_key="201",
                    slurm_job_id="201",
                    result_dir=str(Path(source_plan.run_dir) / "job-201"),
                ),
            )

            with self.assertRaisesRegex(BatchCompileError, r"retry run r1 \(source_record_path=missing\):"):
                _compile_retry_planned_batch(
                    source_batch_root=batch_root,
                    project_root_override=tmp_path,
                    status_query="failed",
                    cli_overrides=[],
                    default_batch_name="retry_default",
                )
