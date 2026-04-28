from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import load_experiment_spec, write_demo_project
import tempfile
import yaml
from pathlib import Path


class TopLevelContractTests(StageBatchSystemTestCase):
    def test_top_level_shape_requires_stages_and_known_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text(
                "project: demo\nexperiment_name: old\nrun: {}\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(Exception, "stages"):
                load_experiment_spec(cfg_path)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["common"] = {"env": {"extra_env": {"OLD": "1"}}}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                Exception, "Unsupported top-level keys: common"
            ):
                load_experiment_spec(cfg_path)

    def test_top_level_matrix_is_rejected_by_unknown_key_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload.pop("runs")
            payload["matrix"] = {"axes": {"train.entry.args.lr": [0.001, 0.002]}}
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                Exception, "Unsupported top-level keys: matrix"
            ):
                load_experiment_spec(cfg_path)
