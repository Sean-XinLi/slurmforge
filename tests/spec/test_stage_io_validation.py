from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    load_experiment_spec,
    write_demo_project,
)
import tempfile
import yaml
from pathlib import Path


class StageIOValidationTests(StageBatchSystemTestCase):
    def test_files_output_discovery_does_not_accept_select(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["outputs"]["train_logs"] = {
                "kind": "files",
                "discover": {"globs": ["logs/**/*.log"], "select": "last"},
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "select.*file outputs"):
                load_experiment_spec(cfg_path)

    def test_eval_input_source_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"]["checkpoint"].pop("source")
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
            with self.assertRaisesRegex(Exception, "source"):
                load_experiment_spec(cfg_path)

    def test_upstream_input_source_must_reference_declared_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"]["checkpoint"]["source"]["output"] = (
                "missing_checkpoint"
            )
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(
                Exception, "missing output `train.missing_checkpoint`"
            ):
                load_experiment_spec(cfg_path)

    def test_input_expects_must_match_upstream_output_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["outputs"]["score"] = {
                "kind": "metric",
                "file": "metrics/train.json",
                "json_path": "$.score",
                "required": True,
            }
            payload["stages"]["eval"]["inputs"]["checkpoint"] = {
                "source": {
                    "kind": "upstream_output",
                    "stage": "train",
                    "output": "score",
                },
                "expects": "path",
                "required": True,
                "inject": {
                    "flag": "score",
                    "env": "SFORGE_INPUT_SCORE",
                    "mode": "path",
                },
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "expects=path.*kind=metric"):
                load_experiment_spec(cfg_path)

    def test_input_inject_mode_must_match_expects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["eval"]["inputs"]["checkpoint"]["inject"]["mode"] = (
                "value"
            )
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            with self.assertRaisesRegex(Exception, "inject.mode.*expects=path"):
                load_experiment_spec(cfg_path)
