from __future__ import annotations

import tempfile
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import load_experiment_spec, write_demo_project


class RunPathTests(StageBatchSystemTestCase):
    def test_cli_set_accepts_stage_shorthand_and_full_stage_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)

            shorthand = load_experiment_spec(
                cfg_path,
                cli_overrides=("train.entry.args.lr=0.004",),
            )
            full_path = load_experiment_spec(
                cfg_path,
                cli_overrides=("stages.train.entry.args.lr=0.004",),
            )

            self.assertEqual(shorthand.stages["train"].entry.args["lr"], 0.004)
            self.assertEqual(
                shorthand.stages["train"].entry.args,
                full_path.stages["train"].entry.args,
            )
