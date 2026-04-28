from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from pathlib import Path


class ControllerBoundaryTests(StageBatchSystemTestCase):
    def test_controller_reads_submission_state_through_public_api(self) -> None:
        source = Path("src/slurmforge/controller/stage_runtime.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("submission._ledger", source)
        self.assertNotIn("from ..submission._ledger", source)
        self.assertNotIn("from ..submission.ledger", source)
        self.assertNotIn("submitted_group_job_ids(", source)
        self.assertIn("read_submission_state", source)
