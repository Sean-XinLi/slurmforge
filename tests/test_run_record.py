from __future__ import annotations

import json
import tempfile
import unittest
import warnings
from dataclasses import replace
from pathlib import Path

from tests._support import sample_run_plan, sample_run_snapshot

from slurmforge.pipeline.planning.contracts import PlanDiagnostic
from slurmforge.pipeline.records import (
    CURRENT_REPLAY_SCHEMA_VERSION,
    RUN_RECORD_EXECUTION_FIELDS,
    RUN_RECORD_OBSERVABILITY_FIELDS,
    RUN_RECORD_TOP_LEVEL_FIELDS,
    deserialize_run_plan,
    deserialize_run_snapshot,
    ensure_replay_spec,
    load_batch_run_plans,
    serialize_run_plan,
    serialize_run_snapshot,
)


class RunRecordTests(unittest.TestCase):
    def test_run_plan_roundtrip_uses_stage_contract_serializer_and_deserializer(self) -> None:
        plan = sample_run_plan(
            planning_diagnostics=(
                PlanDiagnostic(
                    severity="warning",
                    category="resource",
                    code="allocation_above_recommendation",
                    message="too many gpus",
                    stage="train",
                ),
            )
        )

        payload = serialize_run_plan(plan)
        restored = deserialize_run_plan(payload)

        self.assertEqual(restored.cluster.gpus_per_node, plan.cluster.gpus_per_node)
        self.assertEqual(restored.env.extra_env, plan.env.extra_env)
        self.assertEqual(restored.generated_by.name, "slurmforge")
        self.assertEqual(restored.train_stage.topology.nodes, 1)
        self.assertEqual(restored.train_stage.estimate.recommended_total_gpus, 1)
        self.assertEqual(restored.planning_diagnostics[0].code, "allocation_above_recommendation")
        self.assertEqual(set(payload.keys()), set(RUN_RECORD_TOP_LEVEL_FIELDS))
        self.assertEqual(
            set(payload["train_stage"].keys()),
            {
                "name",
                "stage_kind",
                "invocation_kind",
                "launcher_kind",
                "command_text",
                "workdir",
                "topology",
                "allocation",
                "estimate",
                "capabilities",
                "python_bin",
                "launcher_cfg",
                "cluster_cfg",
                "script_path",
                "cli_args",
                "command_mode",
                "requested_launcher_mode",
                "max_gpus_per_job",
                "diagnostics",
            },
        )

    def test_run_snapshot_roundtrip_preserves_replay_contract(self) -> None:
        snapshot = sample_run_snapshot(
            sweep_case_name="oom_retry",
            sweep_assignments={"resources.max_gpus_per_job": 8},
        )

        payload = serialize_run_snapshot(snapshot)
        restored = deserialize_run_snapshot(payload)

        self.assertEqual(restored.generated_by.name, "slurmforge")
        self.assertEqual(restored.replay_spec.planning_root, "/tmp/project_root")
        self.assertEqual(restored.replay_spec.schema_version, CURRENT_REPLAY_SCHEMA_VERSION)
        self.assertEqual(restored.sweep_case_name, "oom_retry")
        self.assertEqual(restored.sweep_assignments, {"resources.max_gpus_per_job": 8})

    def test_replay_spec_warns_on_mismatched_schema_version(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            replay_spec = ensure_replay_spec(
                {
                    "schema_version": 4,
                    "replay_cfg": {"project": "demo"},
                    "planning_root": "/tmp/project_root",
                }
            )

        self.assertEqual(replay_spec.schema_version, 4)
        self.assertTrue(any("schema_version=4" in str(item.message) for item in caught))

    def test_load_batch_run_plans_requires_run_dir_rel(self) -> None:
        plan = replace(
            sample_run_plan(run_id="r2", run_dir="/old/location/batch/runs/run_002_r2"),
            run_dir_rel=None,
        )

        with tempfile.TemporaryDirectory() as tmp:
            batch_root = Path(tmp) / "batch_copy"
            manifest_path = batch_root / "meta" / "runs_manifest.jsonl"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(json.dumps(serialize_run_plan(plan), sort_keys=True) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "run_dir_rel"):
                load_batch_run_plans(batch_root)

    def test_contract_field_groups_cover_top_level_run_record(self) -> None:
        grouped = set(RUN_RECORD_EXECUTION_FIELDS) | set(RUN_RECORD_OBSERVABILITY_FIELDS)
        self.assertEqual(grouped, set(RUN_RECORD_TOP_LEVEL_FIELDS))
