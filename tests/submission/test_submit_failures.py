from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    prepare_stage_submission,
    submit_prepared_stage_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    read_submission_ledger,
    materialize_stage_batch_for_test,
)
import tempfile
from pathlib import Path


class SubmitFailureTests(StageBatchSystemTestCase):
    def test_group_submit_failure_writes_diagnostic(self) -> None:
        from tests.support.slurm import FakeSlurmClient

        class FailingSlurm(FakeSlurmClient):
            def submit(self, path, *, options=None):
                raise RuntimeError("sbatch unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            prepared = prepare_stage_submission(batch)

            with self.assertRaisesRegex(RuntimeError, "sbatch unavailable"):
                submit_prepared_stage_batch(prepared, client=FailingSlurm())

            batch_root = Path(batch.submission_root)
            diagnostic = (
                batch_root
                / "submissions"
                / "diagnostics"
                / "group_001_submit_traceback.log"
            )
            self.assertTrue(diagnostic.exists())
            diagnostic_text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: sbatch unavailable", diagnostic_text)
            self.assertIn("Traceback", diagnostic_text)
            ledger = read_submission_ledger(batch_root)
            assert ledger is not None
            self.assertEqual(ledger.state, "failed")
            self.assertEqual(ledger.groups["group_001"].state, "failed")
            self.assertEqual(ledger.groups["group_001"].reason, "sbatch unavailable")
