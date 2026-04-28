from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import io
import json
import tempfile
import yaml
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class StarterTests(StageBatchSystemTestCase):
    def _init_args(
        self,
        root: Path,
        *,
        template: str = "train-eval",
        force: bool = False,
    ) -> Namespace:
        return Namespace(
            template=template,
            list_templates=False,
            output=str(root / "experiment.yaml"),
            force=force,
        )

    def _interactive_init_args(self) -> Namespace:
        return Namespace(
            template=None,
            list_templates=False,
            output=None,
            force=False,
        )

    def test_templates_generate_loadable_projects(self) -> None:
        from slurmforge.spec import load_experiment_spec
        from slurmforge.starter import (
            InitRequest,
            create_starter_project,
            template_choices,
        )

        expected_orders = {
            "train-eval": ("train", "eval"),
            "train-only": ("train",),
            "eval-checkpoint": ("eval",),
        }
        for template in template_choices():
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = root / "experiment.yaml"
                result = create_starter_project(
                    InitRequest(template=template, output=cfg_path)
                )

                self.assertEqual(result.template, template)
                self.assertTrue(cfg_path.exists())
                self.assertTrue((root / "README.sforge.md").exists())
                spec = load_experiment_spec(cfg_path)
                self.assertEqual(spec.stage_order(), expected_orders[template])

    def test_train_eval_config_fields_and_dry_run_full(self) -> None:
        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = root / "experiment.yaml"
            create_starter_project(InitRequest(template="train-eval", output=cfg_path))

            payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["project"], "demo")
            self.assertEqual(payload["experiment"], "baseline")
            self.assertEqual(payload["storage"]["root"], "./runs")
            self.assertEqual(payload["stages"]["train"]["entry"]["script"], "train.py")
            self.assertEqual(payload["stages"]["eval"]["entry"]["script"], "eval.py")
            self.assertEqual(
                payload["stages"]["train"]["resources"]["partition"], "gpu"
            )
            self.assertEqual(
                payload["runtime"]["executor"]["python"]["bin"], "python3.11"
            )
            self.assertEqual(
                payload["runtime"]["user"]["default"]["python"]["bin"], "python3.11"
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(["run", "--config", str(cfg_path), "--dry-run=full"])

            self.assertEqual(code, 0, stderr.getvalue())
            audit = json.loads(stdout.getvalue())
            self.assertEqual(audit["command"], "run")
            self.assertEqual(audit["state"], "valid")
            self.assertIn("resource_estimate", audit)
            self.assertFalse(
                any((root / "runs").glob("**/train_eval_pipeline_plan.json"))
            )

    def test_all_templates_support_their_full_dry_run_command(self) -> None:
        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        templates = ("train-eval", "train-only", "eval-checkpoint")
        for template in templates:
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = root / "experiment.yaml"
                create_starter_project(InitRequest(template=template, output=cfg_path))
                command = _dry_run_command_for_template(template, root)
                args = [*command, "--config", str(cfg_path), "--dry-run=full"]

                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = main(args)

                self.assertEqual(code, 0, stderr.getvalue())
                self.assertEqual(json.loads(stdout.getvalue())["state"], "valid")

    def test_eval_checkpoint_generates_sample_input_and_readme_note(self) -> None:
        from slurmforge.starter import InitRequest, create_starter_project

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = root / "experiment.yaml"
            create_starter_project(
                InitRequest(template="eval-checkpoint", output=cfg_path)
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

    def test_generated_readme_dry_run_command_stays_executable(self) -> None:
        import os
        import shlex

        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        quickstart = Path("docs/quickstart.md").read_text(encoding="utf-8")
        for template in ("train-eval", "train-only", "eval-checkpoint"):
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = root / "experiment.yaml"
                create_starter_project(InitRequest(template=template, output=cfg_path))
                readme = (root / "README.sforge.md").read_text(encoding="utf-8")
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


def _dry_run_command_for_template(template: str, _root: Path) -> list[str]:
    if template == "train-eval":
        return ["run"]
    if template == "train-only":
        return ["train"]
    return ["eval", "--checkpoint", "checkpoint.pt"]


def _bad_template(file_builder):
    from slurmforge.starter.models import (
        StarterCommandSet,
        StarterReadmePlan,
        StarterTemplate,
    )

    return StarterTemplate(
        name="bad-template",
        description="bad",
        config_builder=lambda _request: {"project": "demo"},
        readme_builder=lambda request: StarterReadmePlan(
            template=request.template,
            commands=StarterCommandSet(
                validate="sforge validate --config experiment.yaml",
                dry_run="sforge run --config experiment.yaml --dry-run=full",
                submit="sforge run --config experiment.yaml",
            ),
            editable_fields=(),
        ),
        file_builders=(file_builder,),
    )
