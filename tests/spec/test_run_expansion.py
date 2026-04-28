from __future__ import annotations

import tempfile
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import load_experiment_spec, write_demo_project


class RunExpansionTests(StageBatchSystemTestCase):
    def test_matrix_run_expansion_keeps_case_grid_identity(self) -> None:
        from slurmforge.spec.run_expansion import expand_run_definitions

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(
                write_demo_project(
                    root,
                    extra={
                        "runs": {
                            "type": "matrix",
                            "cases": [
                                {
                                    "name": "small",
                                    "set": {"train.entry.args.model": "small"},
                                    "axes": {"train.entry.args.lr": [0.001, 0.002]},
                                },
                                {
                                    "name": "large",
                                    "set": {"train.entry.args.model": "large"},
                                    "axes": {"train.entry.args.lr": [0.0001]},
                                },
                            ],
                        },
                    },
                )
            )

            runs = expand_run_definitions(spec)

            self.assertEqual(len(runs), 3)
            self.assertTrue(runs[0].run_id.startswith("small.grid_0001_"))
            self.assertTrue(runs[1].run_id.startswith("small.grid_0002_"))
            self.assertTrue(runs[2].run_id.startswith("large.grid_0001_"))
            self.assertEqual(runs[0].run_overrides["train.entry.args.model"], "small")
            self.assertEqual(runs[2].run_overrides["train.entry.args.model"], "large")
