from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from tests.helpers.overlays import apply_overlay
from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
    read_submission_ledger,
    write_submission_ledger,
)
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    execute_stage_task,
    load_experiment_spec,
    prepare_stage_submission,
    render_pipeline_gate_sbatch,
    write_demo_project,
)


def _with_current_python(extra: dict | None = None) -> dict:
    runtime = {
        "runtime": {
            "executor": {"python": {"bin": sys.executable}},
            "user": {"default": {"python": {"bin": sys.executable}}},
        }
    }
    return apply_overlay(runtime, extra or {})


class PipelineControlTests(StageBatchSystemTestCase):
    def test_initial_submit_creates_short_lived_train_gates_without_gpu_partition(
        self,
    ) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.plans.train_eval import TRAIN_GROUP_GATE
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={"orchestration": {"control": {"partition": None}}},
                )
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)

            result = submit_initial_pipeline(plan, client=FakeSlurmClient())
            pipeline_root = Path(plan.root_dir)
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )
            ledger = json.loads(
                (pipeline_root / "control" / "gate_ledger.json").read_text()
            )
            sbatch = render_pipeline_gate_sbatch(
                plan,
                TRAIN_GROUP_GATE,
                group_id=plan.stage_batches["train"].group_plans[0].group_id,
            )

            self.assertEqual(result.state, "streaming")
            self.assertIn("train_group:group_001", ledger["gates"])
            self.assertEqual(
                set(workflow_state),
                {
                    "schema_version",
                    "pipeline_id",
                    "pipeline_kind",
                    "state",
                    "current_stage",
                    "train_groups",
                    "final_gate",
                },
            )
            self.assertNotIn("#SBATCH --partition=gpu", sbatch)
            self.assertNotIn("#SBATCH --partition", sbatch)

    def test_train_group_gate_streams_eval_shard_and_queues_final_gate(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.plans.train_eval import TRAIN_GROUP_GATE
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(root, extra=_with_current_python())
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0][2]
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")

            result = advance_pipeline_once(
                Path(plan.root_dir),
                gate=TRAIN_GROUP_GATE,
                group_id="group_001",
                client=client,
                missing_output_grace_seconds=0,
            )

            pipeline_root = Path(plan.root_dir)
            eval_shard = (
                pipeline_root / "stage_batches" / "eval" / "shards" / "group_001"
            )
            ledger = json.loads(
                (pipeline_root / "control" / "gate_ledger.json").read_text()
            )
            catalog = json.loads(
                (pipeline_root / "execution" / "stage_catalog.json").read_text()
            )
            runtime_batches = json.loads(
                (pipeline_root / "execution" / "runtime_batches.json").read_text()
            )
            workflow_state = json.loads(
                (pipeline_root / "control" / "workflow_state.json").read_text()
            )

            self.assertEqual(result.state, "final_gate_submitted")
            self.assertTrue((eval_shard / "selected_batch_plan.json").exists())
            self.assertEqual(
                set(ledger["gates"]),
                {"train_group:group_001", "eval_shard:group_001", "final"},
            )
            self.assertEqual(
                [
                    (batch["stage_name"], batch["role"], batch["shard_id"])
                    for batch in catalog["batches"]
                ],
                [
                    ("train", "pipeline_stage", ""),
                    ("eval", "pipeline_stage", ""),
                ],
            )
            self.assertEqual(
                [
                    (batch["stage_name"], batch["role"], batch["shard_id"])
                    for batch in runtime_batches["batches"]
                ],
                [
                    ("train", "pipeline_entry", ""),
                    ("eval", "eval_shard", "group_001"),
                ],
            )
            group = workflow_state["train_groups"]["group_001"]
            self.assertEqual(
                group["terminal_dependency_gate_key"], "eval_shard:group_001"
            )
            self.assertNotIn("train_job_id", group)
            self.assertNotIn("eval_job_ids", group)

    def test_train_submission_reuses_partial_stage_submission_without_duplicate(
        self,
    ) -> None:
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra=_with_current_python(
                    {
                        "runs": {
                            "type": "grid",
                            "axes": {"train.resources.constraint": ["a", "b"]},
                        },
                        "dispatch": {
                            "max_available_gpus": 2,
                            "overflow_policy": "serialize_groups",
                        },
                    }
                ),
            )
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            train_batch = plan.stage_batches["train"]
            first_group = train_batch.group_plans[0].group_id
            second_group = train_batch.group_plans[1].group_id
            prepare_stage_submission(train_batch)
            ledger = read_submission_ledger(Path(train_batch.submission_root))
            assert ledger is not None
            ledger.state = "partial"
            ledger.groups[first_group].state = "submitted"
            ledger.groups[first_group].scheduler_job_id = "111"
            write_submission_ledger(Path(train_batch.submission_root), ledger)

            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)

            stage_submissions = [
                path.name
                for path, _, _ in client.submissions
                if path.name.endswith(".sbatch")
            ]
            self.assertIn(f"{second_group}.sbatch", stage_submissions)
            self.assertNotIn(f"{first_group}.sbatch", stage_submissions)

    def test_final_gate_sends_one_pipeline_terminal_notification(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from slurmforge.notifications.records import read_notification_record
        from slurmforge.plans.train_eval import EVAL_SHARD_GATE, FINAL_GATE
        from slurmforge.plans.train_eval import TRAIN_GROUP_GATE
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra=_with_current_python(
                    {
                        "notifications": {
                            "email": {
                                "enabled": True,
                                "to": ["you@example.com"],
                                "on": ["train_eval_pipeline_finished"],
                                "mode": "summary",
                            }
                        },
                    }
                ),
            )
            spec = load_experiment_spec(cfg_path)
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)
            train_job_id = client.submissions[0][2]
            train_root = Path(plan.stage_batches["train"].submission_root)
            self.assertEqual(execute_stage_task(train_root, 1, 0), 0)
            client.set_array_task_state(train_job_id, 0, "COMPLETED")
            advance_pipeline_once(
                pipeline_root,
                gate=TRAIN_GROUP_GATE,
                group_id="group_001",
                client=client,
                missing_output_grace_seconds=0,
            )
            eval_shard = (
                pipeline_root / "stage_batches" / "eval" / "shards" / "group_001"
            )
            eval_job_id = next(
                job_id
                for path, _, job_id in client.submissions
                if path.is_relative_to(eval_shard / "submit")
            )
            self.assertEqual(execute_stage_task(eval_shard, 1, 0), 0)
            client.set_array_task_state(eval_job_id, 0, "COMPLETED")
            advance_pipeline_once(
                pipeline_root,
                gate=EVAL_SHARD_GATE,
                group_id="group_001",
                client=client,
                missing_output_grace_seconds=0,
            )

            sent: list[dict] = []

            def fake_send(**kwargs):
                sent.append(kwargs)

            with patch(
                "slurmforge.notifications.delivery.send_email_summary",
                side_effect=fake_send,
            ):
                advance_pipeline_once(
                    pipeline_root,
                    gate=FINAL_GATE,
                    client=client,
                    missing_output_grace_seconds=0,
                )
                advance_pipeline_once(
                    pipeline_root,
                    gate=FINAL_GATE,
                    client=client,
                    missing_output_grace_seconds=0,
                )

            self.assertEqual(len(sent), 1)
            self.assertIn("SlurmForge train/eval pipeline finished", sent[0]["body"])
            record = read_notification_record(
                pipeline_root, "train_eval_pipeline_finished"
            )
            assert record is not None
            self.assertEqual(record.state, "sent")
            self.assertEqual(record.recipients, ("you@example.com",))
