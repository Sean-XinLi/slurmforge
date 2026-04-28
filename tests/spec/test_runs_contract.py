from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import load_experiment_spec, write_demo_project
import tempfile
import yaml
from pathlib import Path


class RunsContractTests(StageBatchSystemTestCase):
    def test_case_runs_require_valid_unique_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runs"] = {
                "type": "cases",
                "cases": [
                    {"name": "bad/name", "set": {"train.entry.args.lr": 0.001}},
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                Exception, "letters, numbers, underscores, dots, and dashes"
            ):
                load_experiment_spec(cfg_path)

            payload["runs"] = {
                "type": "cases",
                "cases": [
                    {"name": "dup", "set": {"train.entry.args.lr": 0.001}},
                    {"name": "dup", "set": {"train.entry.args.lr": 0.002}},
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "must be unique: dup"):
                load_experiment_spec(cfg_path)

    def test_case_runs_validate_set_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runs"] = {
                "type": "cases",
                "cases": [
                    {"name": "bad_path", "set": {"train.resources.missing": 1}},
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                Exception, "runs.cases.bad_path.set.train.resources.missing"
            ):
                load_experiment_spec(cfg_path)

    def test_matrix_runs_validate_case_axes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runs"] = {
                "type": "matrix",
                "cases": [
                    {
                        "name": "small_model",
                        "set": {"train.entry.args.model": "small"},
                        "axes": {
                            "train.entry.args.lr": [0.001, 0.002],
                            "train.entry.args.seed": [1, 2],
                        },
                    }
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            spec = load_experiment_spec(cfg_path)
            self.assertEqual(spec.runs.type, "matrix")
            self.assertEqual(spec.runs.cases[0].name, "small_model")
            self.assertEqual(
                tuple(path for path, _values in spec.runs.cases[0].axes),
                (
                    "train.entry.args.lr",
                    "train.entry.args.seed",
                ),
            )

            payload["runs"]["axes"] = {"train.entry.args.lr": [0.003]}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "defines axes under each case"):
                load_experiment_spec(cfg_path)

            payload["runs"].pop("axes")
            payload["runs"]["cases"][0].pop("axes")
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "runs.cases.small_model.axes"):
                load_experiment_spec(cfg_path)

            payload["runs"]["cases"][0]["axes"] = {"train.resources.missing": [1, 2]}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                Exception, "runs.cases.small_model.axes.train.resources.missing"
            ):
                load_experiment_spec(cfg_path)

    def test_matrix_runs_reject_set_and_axis_collisions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["runs"] = {
                "type": "matrix",
                "cases": [
                    {
                        "name": "conflict",
                        "set": {"train.entry.args.lr": 0.001},
                        "axes": {"stages.train.entry.args.lr": [0.002]},
                    }
                ],
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                Exception, "cannot set and sweep the same path"
            ):
                load_experiment_spec(cfg_path)
