from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from slurmforge.pipeline.compiler import ReplaySourceRequest, compile_source
from slurmforge.pipeline.compiler.reports import (
    report_planned_run_count,
    report_total_failed_runs,
    report_total_runs,
    require_success,
)
from slurmforge.pipeline.config.codecs import normalize_storage_config
from slurmforge.pipeline.materialization import materialize_batch
from slurmforge.pipeline.planning import BatchIdentity, PlannedBatch, PlannedRun
from slurmforge.pipeline.records import DispatchInfo, build_replay_spec, serialize_run_plan, serialize_run_snapshot
from slurmforge.pipeline.sources.replay import (
    collect_replay_source_inputs,
)
from slurmforge.pipeline.sources.models import SourceInputBatch, SourceRef, SourceRunInput
from slurmforge.storage import create_planning_store
from tests._support import make_template_env, sample_run_plan, sample_run_snapshot, sample_stage_plan, write_test_descriptor


def _command_replay_cfg(*, lr: float = 0.001) -> dict:
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
            "gpus_per_node": 1,
            "cpus_per_task": 2,
            "mem": "0",
        },
        "resources": {
            "auto_gpu": False,
            "max_available_gpus": 2,
            "max_gpus_per_job": 2,
        },
    }


def _load_replay_inputs_from_batch_root(
    batch_root: Path,
    *,
    run_ids: list[str] | tuple[str, ...] = (),
    run_indices: list[int] | tuple[int, ...] = (),
) -> list[SourceRunInput]:
    collection = collect_replay_source_inputs(
        source_run_dir=None,
        source_batch_root=batch_root,
        run_ids=run_ids,
        run_indices=run_indices,
    )
    return list(collection.source_inputs)


def _compile_replay_planned_batch(
    replay_inputs: list[SourceRunInput] | tuple[SourceRunInput, ...],
    *,
    project_root_override: Path | None,
    cli_overrides: list[str] | tuple[str, ...],
    default_batch_name: str,
    manifest_extras: dict | None = None,
    manifest_context_key: str | None = None,
):
    del manifest_context_key
    collection = SourceInputBatch(
        source_inputs=tuple(replay_inputs),
        failed_runs=(),
        checked_inputs=len(tuple(replay_inputs)),
        batch_diagnostics=(),
        manifest_extras={} if manifest_extras is None else manifest_extras,
        source_summary="batch=<patched replay source>",
    )
    with patch("slurmforge.pipeline.compiler.flows.replay.collect.collect_replay_source_inputs", return_value=collection):
        return require_success(
            compile_source(
            ReplaySourceRequest(
                source_batch_root=Path("/patched-replay-source"),
                cli_overrides=tuple(cli_overrides),
                project_root=project_root_override,
                default_batch_name=default_batch_name,
            )
            )
        )


def _materialize_replay_batch(
    tmp_path: Path,
    *,
    storage_cfg_dict: dict | None = None,
) -> tuple[Path, Path]:
    storage_config = normalize_storage_config(storage_cfg_dict)
    identity = BatchIdentity(
        project_root=tmp_path,
        base_output_dir=tmp_path / "runs",
        project="demo",
        experiment_name="exp",
        batch_name="replay_src",
    )
    batch_root = identity.batch_root
    run_dir = batch_root / "runs" / "run_001_r1"
    plan = sample_run_plan(
        run_index=1,
        total_runs=1,
        run_id="r1",
        run_dir=str(run_dir),
        run_dir_rel="runs/run_001_r1",
        dispatch=DispatchInfo(record_path_rel="records/group_01/task_000000.json"),
        train_stage=sample_stage_plan(workdir=tmp_path),
    )
    snapshot = sample_run_snapshot(
        run_index=1,
        total_runs=1,
        run_id="r1",
        replay_spec=build_replay_spec(
            _command_replay_cfg(lr=0.001),
            planning_root=str(tmp_path),
            source_batch_root=str(batch_root),
            source_run_id="r1",
            source_record_path=str(batch_root / "records" / "group_01" / "task_000000.json"),
        ),
    )
    planned_batch = PlannedBatch(
        identity=identity,
        planned_runs=(PlannedRun(plan=plan, snapshot=snapshot),),
        storage_config=storage_config,
    )
    env = make_template_env()
    store = create_planning_store(storage_config, env)
    materialize_batch(planned_batch=planned_batch, planning_store=store)
    return batch_root, run_dir


class ReplayTests(unittest.TestCase):
    def test_load_replay_inputs_from_batch_root_filters_selected_run_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)

            plan1 = sample_run_plan(
                run_index=1,
                total_runs=2,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                run_dir_rel="runs/run_001_r1",
                dispatch=DispatchInfo(record_path_rel="records/group_01/task_000000.json"),
            )
            plan2 = sample_run_plan(
                run_index=2,
                total_runs=2,
                run_id="r2",
                run_dir=str(batch_root / "runs" / "run_002_r2"),
                run_dir_rel="runs/run_002_r2",
                dispatch=DispatchInfo(record_path_rel="records/group_01/task_000001.json"),
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

            for plan in (plan1, plan2):
                run_dir = Path(plan.run_dir)
                run_dir.mkdir(parents=True, exist_ok=True)
                snapshot = sample_run_snapshot(
                    run_index=plan.run_index,
                    total_runs=2,
                    run_id=plan.run_id,
                    replay_spec=build_replay_spec(
                        _command_replay_cfg(lr=0.001 * plan.run_index),
                        planning_root=str(tmp_path),
                        source_batch_root=str(batch_root),
                        source_run_id=plan.run_id,
                        source_record_path=str(batch_root / "records" / "group_01" / f"task_{plan.run_index - 1:06d}.json"),
                    ),
                )
                snapshot_path = run_dir / "meta" / "run_snapshot.json"
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                snapshot_path.write_text(
                    json.dumps(serialize_run_snapshot(snapshot), sort_keys=True),
                    encoding="utf-8",
                )

            replay_inputs = _load_replay_inputs_from_batch_root(
                batch_root,
                run_ids=["r2"],
            )

        self.assertEqual(len(replay_inputs), 1)
        self.assertEqual(replay_inputs[0].source.source_run_id, "r2")
        self.assertEqual(replay_inputs[0].original_run_index, 2)
        self.assertEqual(replay_inputs[0].source.source_batch_root, batch_root.resolve())
        self.assertTrue(str(replay_inputs[0].source.config_label).startswith("replay run r2"))

    def test_load_replay_inputs_from_run_dir_uses_storage_in_sqlite_pure_db_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root, run_dir = _materialize_replay_batch(
                tmp_path,
                storage_cfg_dict={
                    "backend": {"engine": "sqlite"},
                    "exports": {"planning_recovery": False},
                },
            )

            collection = collect_replay_source_inputs(
                source_run_dir=run_dir,
                source_batch_root=None,
                run_ids=[],
                run_indices=[],
            )

        self.assertEqual(collection.checked_inputs, 1)
        self.assertEqual(len(collection.failed_runs), 0)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_batch_root, batch_root.resolve())
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r1")
        self.assertIsNone(collection.source_inputs[0].source.config_path)
        self.assertFalse((run_dir / "meta" / "run_snapshot.json").exists())

    def test_load_replay_inputs_from_batch_root_uses_storage_in_sqlite_pure_db_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root, run_dir = _materialize_replay_batch(
                tmp_path,
                storage_cfg_dict={
                    "backend": {"engine": "sqlite"},
                    "exports": {"planning_recovery": False},
                },
            )

            collection = collect_replay_source_inputs(
                source_run_dir=None,
                source_batch_root=batch_root,
                run_ids=["r1"],
                run_indices=[],
            )

        self.assertEqual(collection.checked_inputs, 1)
        self.assertEqual(len(collection.failed_runs), 0)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_batch_root, batch_root.resolve())
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r1")
        # In pure DB mode (planning_recovery=false), record files don't exist.
        # source_record_path and config_path must be None, not dangling paths.
        self.assertIsNone(collection.source_inputs[0].source.config_path)
        self.assertIsNone(collection.source_inputs[0].source.source_record_path)
        self.assertTrue(str(collection.source_inputs[0].source.config_label).startswith("replay run r1"))
        self.assertFalse((batch_root / "records").exists())
        self.assertFalse((run_dir / "meta" / "run_snapshot.json").exists())

    def test_build_replay_planned_batch_applies_overrides_and_augments_manifest_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            replay_input = SourceRunInput(
                source_kind="replay",
                source_index=1,
                run_cfg=_command_replay_cfg(lr=0.002),
                source=SourceRef(
                    config_path=None,
                    config_label="replay run r1",
                    planning_root=str(tmp_path),
                    source_batch_root=tmp_path / "batch_src",
                    source_run_id="r1",
                    source_record_path=None,
                ),
                original_run_index=7,
            )

            planned_batch = _compile_replay_planned_batch(
                [replay_input],
                project_root_override=None,
                cli_overrides=["cluster.mem=128G"],
                default_batch_name="replay_batch",
                manifest_extras={"replay_source": {"source_kind": "run"}},
                manifest_context_key="replay_source",
            )

        self.assertEqual(planned_batch.total_runs, 1)
        self.assertEqual(planned_batch.batch_name, "replay_batch")
        self.assertEqual(planned_batch.planned_runs[0].plan.cluster.mem, "128G")
        self.assertEqual(planned_batch.manifest_extras["replay_source"]["planning_root"], str(tmp_path.resolve()))
        self.assertEqual(planned_batch.manifest_extras["replay_source"]["cli_overrides"], ["cluster.mem=128G"])
        self.assertEqual(planned_batch.manifest_extras["replay_source"]["selected_run_ids"], ["r1"])
        self.assertEqual(planned_batch.manifest_extras["replay_source"]["selected_run_indices"], [7])

    def test_collect_replay_source_inputs_keeps_loading_after_bad_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            batch_root = tmp_path / "batch_src"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            write_test_descriptor(batch_root)

            plan1 = sample_run_plan(
                run_index=1,
                total_runs=2,
                run_id="r1",
                run_dir=str(batch_root / "runs" / "run_001_r1"),
                dispatch=DispatchInfo(record_path_rel="records/group_01/task_000000.json"),
            )
            plan2 = sample_run_plan(
                run_index=2,
                total_runs=2,
                run_id="r2",
                run_dir=str(batch_root / "runs" / "run_002_r2"),
                dispatch=DispatchInfo(record_path_rel="records/group_01/task_000001.json"),
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

            bad_snapshot_path = Path(plan1.run_dir) / "meta" / "run_snapshot.json"
            bad_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            bad_snapshot_path.write_text("{not-json", encoding="utf-8")

            good_run_dir = Path(plan2.run_dir)
            good_run_dir.mkdir(parents=True, exist_ok=True)
            good_snapshot = sample_run_snapshot(
                run_index=2,
                total_runs=2,
                run_id="r2",
                replay_spec=build_replay_spec(
                    _command_replay_cfg(lr=0.002),
                    planning_root=str(tmp_path),
                    source_batch_root=str(batch_root),
                    source_run_id="r2",
                    source_record_path=str(batch_root / "records" / "group_01" / "task_000001.json"),
                ),
            )
            good_snapshot_path = good_run_dir / "meta" / "run_snapshot.json"
            good_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            good_snapshot_path.write_text(
                json.dumps(serialize_run_snapshot(good_snapshot), sort_keys=True),
                encoding="utf-8",
            )

            collection = collect_replay_source_inputs(
                source_run_dir=None,
                source_batch_root=batch_root,
                run_ids=[],
                run_indices=[],
            )

        self.assertEqual(collection.checked_inputs, 2)
        self.assertEqual(len(collection.failed_runs), 1)
        self.assertEqual(len(collection.source_inputs), 1)
        self.assertEqual(collection.source_inputs[0].source.source_run_id, "r2")
        self.assertEqual(collection.source_inputs[0].source_index, 2)
        self.assertEqual(collection.failed_runs[0].phase, "source")
        self.assertIn("replay run r1", collection.failed_runs[0].diagnostics[0].message)

    def test_compile_source_collects_failures_without_stopping_at_first_error_for_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            replay_inputs = [
                SourceRunInput(
                    source_kind="replay",
                    source_index=1,
                    run_cfg={
                        "project": "demo",
                        "experiment_name": "exp",
                        "resolved_model_catalog": {"models": []},
                        "run": "not-a-mapping",
                    },
                    source=SourceRef(
                        config_path=None,
                        config_label="replay run broken",
                        planning_root=str(tmp_path),
                        source_batch_root=tmp_path / "batch_src",
                        source_run_id="r-bad",
                        source_record_path=None,
                    ),
                ),
                SourceRunInput(
                    source_kind="replay",
                    source_index=2,
                    run_cfg=_command_replay_cfg(lr=0.003),
                    source=SourceRef(
                        config_path=None,
                        config_label="replay run ok",
                        planning_root=str(tmp_path),
                        source_batch_root=tmp_path / "batch_src",
                        source_run_id="r-ok",
                        source_record_path=None,
                    ),
                    original_run_index=2,
                ),
            ]

            collection = SourceInputBatch(
                source_inputs=tuple(replay_inputs),
                failed_runs=(),
                checked_inputs=2,
                batch_diagnostics=(),
                manifest_extras={"replay_source": {"source_kind": "batch"}},
                source_summary="batch=<patched replay source>",
            )
            with patch("slurmforge.pipeline.compiler.flows.replay.collect.collect_replay_source_inputs", return_value=collection):
                report = compile_source(
                    ReplaySourceRequest(
                        source_batch_root=Path("/patched-replay-source"),
                        default_batch_name="replay_batch",
                    )
                )

        self.assertEqual(report_total_runs(report), 2)
        self.assertEqual(report_total_failed_runs(report), 1)
        self.assertEqual(report_planned_run_count(report), 1)
        self.assertIn("replay run broken:", report.failed_runs[0].diagnostics[0].message)
        self.assertEqual(report.successful_runs[0].snapshot.replay_spec.source_run_id, "r-ok")
        self.assertEqual(report.manifest_extras["replay_source"]["selected_run_ids"], ["r-bad", "r-ok"])
        self.assertEqual(report.source_summary, "batch=<patched replay source>")
