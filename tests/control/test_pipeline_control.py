from __future__ import annotations

import json
import tempfile
from pathlib import Path

from tests.control.pipeline_overlays import with_current_python
from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_train_eval_pipeline_for_test
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    load_experiment_spec,
    write_demo_project,
)


class PipelineControlTests(StageBatchSystemTestCase):
    def test_controller_advance_failure_records_terminal_event(self) -> None:
        from slurmforge.control.workflow import advance_pipeline_once
        from slurmforge.control.workflow import submit_initial_pipeline
        from tests.support.slurm import FakeSlurmClient

        class FailingObservedQuerySlurm(FakeSlurmClient):
            def __init__(self, source: FakeSlurmClient) -> None:
                super().__init__()
                self._next_job_id = source._next_job_id
                self.submissions = list(source.submissions)
                self.job_states = dict(source.job_states)

            def query_observed_jobs(self, job_ids):
                raise RuntimeError("scheduler query unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(root, extra=with_current_python())
            )
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            pipeline_root = Path(plan.root_dir)
            client = FakeSlurmClient()
            submit_initial_pipeline(plan, client=client)

            with self.assertRaises(RuntimeError):
                advance_pipeline_once(
                    pipeline_root,
                    client=FailingObservedQuerySlurm(client),
                    missing_output_grace_seconds=0,
                )

            events = [
                json.loads(line)
                for line in (pipeline_root / "control" / "events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertTrue(
                any(
                    event["event"] == "controller_advance_failed"
                    and event["workflow_state_after"] == "failed"
                    and "scheduler query unavailable" in event["reason"]
                    for event in events
                )
            )
