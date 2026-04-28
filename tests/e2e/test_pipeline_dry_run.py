from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    write_demo_project,
)
import json
import tempfile
from argparse import Namespace
from pathlib import Path


class PipelineDryRunTests(StageBatchSystemTestCase):
    def test_machine_dry_run_full_emits_auditable_json_without_materializing(
        self,
    ) -> None:
        from slurmforge.cli.train import handle_train

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            audit_path = root / "audit.json"
            handle_train(
                Namespace(
                    config=str(cfg_path),
                    set=[],
                    project_root=None,
                    dry_run="full",
                    emit_only=False,
                    output=str(audit_path),
                )
            )
            payload = json.loads(audit_path.read_text())
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["command"], "train")
            self.assertEqual(payload["state"], "valid")
            self.assertEqual(
                payload["validation"]["runtime_contracts"][0]["state"], "verified"
            )
            probe_roles = {
                item["runtime_role"]: item
                for item in payload["validation"]["runtime_contracts"][0]["probes"]
            }
            self.assertEqual(probe_roles["executor"]["state"], "verified")
            self.assertEqual(probe_roles["user"]["state"], "verified")
            self.assertFalse(any((root / "runs").glob("**/batch_plan.json")))
