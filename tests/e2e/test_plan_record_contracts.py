from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_stage_batch_for_test,
)
import json
import tempfile
import yaml
from pathlib import Path


class PlanRecordContractTests(StageBatchSystemTestCase):
    def test_plan_files_have_schema_versions_and_typed_output_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            cfg = yaml.safe_load(cfg_path.read_text())
            cfg["stages"]["train"]["outputs"]["train_logs"] = {
                "kind": "files",
                "discover": {"globs": ["logs/**/*.log"]},
            }
            cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            batch_payload = json.loads(
                (Path(train_batch.submission_root) / "batch_plan.json").read_text()
            )
            self.assertEqual(batch_payload["schema_version"], 1)
            self.assertEqual(batch_payload["stage_instances"][0]["schema_version"], 1)
            self.assertEqual(batch_payload["group_plans"][0]["schema_version"], 1)
            output_contract = batch_payload["stage_instances"][0]["output_contract"]
            self.assertEqual(output_contract["schema_version"], 1)
            self.assertEqual(
                output_contract["outputs"]["checkpoint"]["schema_version"], 1
            )
            self.assertEqual(
                output_contract["outputs"]["checkpoint"]["discover"]["select"],
                "latest_step",
            )
            self.assertNotIn(
                "select", output_contract["outputs"]["train_logs"]["discover"]
            )

            run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            stage_plan = json.loads((run_dir / "stage_plan.json").read_text())
            self.assertEqual(
                stage_plan["output_contract"]["outputs"]["checkpoint"]["discover"][
                    "globs"
                ],
                ["checkpoints/**/*.pt"],
            )
