from __future__ import annotations

from pathlib import Path
import json
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


class ExecutorRuntimeContractTests(StageBatchSystemTestCase):
    def test_executor_runtime_contract_failure_blocks_user_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runtime"]["user"]["default"]["python"]["min_version"] = "99.0"
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "(Path('user_script_started.txt')).write_text('started')",
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

            self.assertNotEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )

            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            status = json.loads((train_run_dir / "status.json").read_text())
            probe = json.loads(
                (train_run_dir / "attempts" / "0001" / "runtime_probe.json").read_text()
            )
            probe_states = {
                item["runtime_role"]: item["state"] for item in probe["probes"]
            }
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "runtime_contract_error")
            self.assertEqual(probe_states["user"], "failed")
            self.assertFalse((root / "user_script_started.txt").exists())
