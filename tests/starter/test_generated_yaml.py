from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import tempfile
import yaml
from pathlib import Path


class GeneratedYamlTests(StageBatchSystemTestCase):
    def test_generated_yaml_includes_inline_option_hints(self) -> None:
        from slurmforge.starter import InitRequest, create_starter_project
        from slurmforge.starter.config_comments import (
            inline_comment_for,
            option_comment,
        )

        for template in ("train-eval", "train-only", "eval-checkpoint"):
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = root / "experiment.yaml"
                create_starter_project(InitRequest(template=template, output_dir=root))

                text = cfg_path.read_text(encoding="utf-8")
                payload = yaml.safe_load(text)
                self.assertIn("# Starter template: " + template, text)
                self.assertIn(option_comment("runs.type", indent=2).strip(), text)
                self.assertIn(
                    option_comment("stages.*.launcher.type", indent=6).strip(),
                    text,
                )
                self.assertIn(
                    option_comment("stages.*.outputs.*.kind", indent=8).strip(),
                    text,
                )
                self.assertIn(
                    option_comment("artifact_store.strategy", indent=2).strip(),
                    text,
                )
                self.assertIn(
                    option_comment("notifications.email.events", indent=4).strip(),
                    text,
                )
                self.assertIn(
                    option_comment("notifications.email.when", indent=4).strip(),
                    text,
                )
                self.assertIn(
                    f'min_version: "3.10"  # {inline_comment_for("runtime.executor.python.min_version")}',
                    text,
                )
                self.assertIn(
                    f'min_version: "3.10"  # {inline_comment_for("runtime.user.*.python.min_version")}',
                    text,
                )
                if template == "eval-checkpoint":
                    self.assertIn(
                        "This starter writes checkpoint.pt as a sample input.",
                        text,
                    )
                self.assertEqual(
                    payload["runtime"]["executor"]["python"]["min_version"],
                    "3.10",
                )
                self.assertFalse(payload["notifications"]["email"]["enabled"])
                self.assertEqual(
                    payload["notifications"]["email"]["events"], ["batch_finished"]
                )
