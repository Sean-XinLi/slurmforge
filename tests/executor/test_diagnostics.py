from __future__ import annotations

from pathlib import Path
import tempfile
from unittest.mock import patch

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_stage_batch_for_test
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
)


class ExecutorDiagnosticTests(StageBatchSystemTestCase):
    def test_executor_unknown_error_writes_traceback_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            with patch(
                "slurmforge.executor.runner.build_shell_script",
                side_effect=RuntimeError("executor boom"),
            ):
                self.assertNotEqual(
                    execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
                )

            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            diagnostic = (
                train_run_dir / "attempts" / "0001" / "logs" / "executor_traceback.log"
            )
            self.assertTrue(diagnostic.exists())
            text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: executor boom", text)
            self.assertIn("Traceback", text)
