from __future__ import annotations

from pathlib import Path
import tempfile

import yaml

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_stage_batch_for_test
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
)


class ExecutorBeforeStepTests(StageBatchSystemTestCase):
    def test_executor_runs_before_steps_with_stage_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["before"] = [
                {
                    "name": "prepare-marker",
                    "run": "printf '%s' \"$DEMO_ENV\" > before_marker.txt",
                },
            ]
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "assert Path('before_marker.txt').read_text() == '1'",
                        "Path('checkpoints').mkdir(exist_ok=True)",
                        "(Path('checkpoints') / 'step_1.pt').write_text('ckpt')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)

            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )

            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            attempt_dir = train_run_dir / "attempts" / "0001"
            self.assertTrue((root / "before_marker.txt").exists())
            self.assertTrue((attempt_dir / "environment_plan.json").exists())
            self.assertTrue((attempt_dir / "before_steps.json").exists())
