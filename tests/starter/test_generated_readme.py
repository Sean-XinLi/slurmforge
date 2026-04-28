from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
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

    def test_generated_readme_common_options_and_dry_run_command_stay_executable(
        self,
    ) -> None:
        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        quickstart = Path("docs/quickstart.md").read_text(encoding="utf-8")
        for template in ("train-eval", "train-only", "eval-checkpoint"):
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                create_starter_project(InitRequest(template=template, output_dir=root))
                readme = (root / "README.sforge.md").read_text(encoding="utf-8")
                self.assertIn("## Connect Your Model", readme)
                self.assertIn("SECTION A - SlurmForge contract", readme)
                self.assertIn("SECTION B - Your model code", readme)
                self.assertIn("SECTION C - Output contract", readme)
                self.assertIn("SFORGE_INPUT_CHECKPOINT", readme)
                self.assertIn("eval/metrics.json", readme)
                self.assertIn("## Common Field Options", readme)
                self.assertIn("| Field | Options | Meaning |", readme)
                self.assertIn("`matrix`: Plan named cases, each with its own grid.", readme)
                self.assertIn(
                    "`notifications.email.on` | `batch_finished`, `train_eval_pipeline_finished`",
                    readme,
                )
                self.assertIn(
                    "`stages.*.inputs.*.source.kind` | `upstream_output`, `external_path`",
                    readme,
                )
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
