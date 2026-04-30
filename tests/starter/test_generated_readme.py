from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.starter.helpers import use_current_python_for_dry_run
import io
import json
import os
import shlex
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class GeneratedReadmeTests(StageBatchSystemTestCase):
    def test_eval_checkpoint_generates_sample_input_and_readme_note(self) -> None:
        from slurmforge.starter import InitRequest, create_starter_project

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            create_starter_project(
                InitRequest(template="eval-checkpoint", output_dir=root)
            )

            self.assertEqual(
                (root / "checkpoint.pt").read_text(encoding="utf-8"),
                "sample checkpoint for sforge init\n",
            )
            readme = (root / "README.sforge.md").read_text(encoding="utf-8")
            self.assertIn("Template: `eval-checkpoint`", readme)
            self.assertIn("`checkpoint.pt` is a sample input file", readme)
            self.assertIn(
                "Relative `--checkpoint` paths are resolved from the config directory.",
                readme,
            )
            self.assertIn("`stages.eval.entry.script`", readme)
            self.assertNotIn("`stages.train.entry.script`", readme)
            self.assertNotIn("The train stage must leave", readme)

    def test_generated_readme_is_workflow_scoped_and_dry_run_command_stays_executable(
        self,
    ) -> None:
        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        quickstart = Path("docs/quickstart.md").read_text(encoding="utf-8")
        for template in ("train-eval", "train-only", "eval-checkpoint"):
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                create_starter_project(InitRequest(template=template, output_dir=root))
                use_current_python_for_dry_run(root / "experiment.yaml")
                readme = (root / "README.sforge.md").read_text(encoding="utf-8")
                config_guide = (root / "CONFIG.sforge.md").read_text(encoding="utf-8")
                self.assertIn("## Connect Your Model", readme)
                self.assertIn("SECTION A - SlurmForge contract", readme)
                self.assertIn("SECTION B - Your model code", readme)
                self.assertIn("SECTION C - Output contract", readme)
                self.assertIn("## Edit These First", readme)
                self.assertIn("`CONFIG.sforge.md`: fields used by this starter.", readme)
                self.assertNotIn("## Common Field Options", readme)
                self.assertNotIn("| Field | Options | Meaning |", readme)
                self.assertNotIn("`notifications.email.on`", readme)
                self.assertIn("# Starter Config Guide", config_guide)
                self.assertIn("Template: `" + template + "`", config_guide)
                self.assertIn("`runs.type`", config_guide)
                self.assertIn("`matrix`: Plan named cases, each with its own grid.", config_guide)
                self.assertIn(
                    "`notifications.email.on`",
                    config_guide,
                )
                self.assertIn(
                    "`stages.*.outputs.*.kind`",
                    config_guide,
                )
                if template == "train-only":
                    self.assertIn("The train stage must leave", readme)
                    self.assertNotIn("SFORGE_INPUT_CHECKPOINT", readme)
                    self.assertNotIn("eval/metrics.json", readme)
                    self.assertIn("`stages.train.entry.script`", config_guide)
                    self.assertNotIn("`stages.eval.entry.script`", config_guide)
                    self.assertNotIn("`stages.eval.inputs.checkpoint`", config_guide)
                else:
                    self.assertIn("SFORGE_INPUT_CHECKPOINT", readme)
                    self.assertIn("eval/metrics.json", readme)
                if template == "eval-checkpoint":
                    self.assertIn("`stages.eval.entry.script`", config_guide)
                    self.assertNotIn("`stages.train.entry.script`", config_guide)
                    self.assertNotIn("`stages.train.outputs.checkpoint`", config_guide)
                command = next(
                    line for line in readme.splitlines() if "--dry-run=full" in line
                )
                self.assertIn(command, quickstart)
                args = shlex.split(command)
                self.assertEqual(args[0], "sforge")
                cwd = Path.cwd()
                stdout = io.StringIO()
                stderr = io.StringIO()
                try:
                    os.chdir(root)
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        code = main(args[1:])
                finally:
                    os.chdir(cwd)

                self.assertEqual(code, 0, stderr.getvalue())
                self.assertEqual(json.loads(stdout.getvalue())["state"], "valid")
