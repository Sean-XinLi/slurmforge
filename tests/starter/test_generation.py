from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.starter.helpers import (
    dry_run_command_for_template,
    use_current_python_for_dry_run,
)
import io
import json
import tempfile
import yaml
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class StarterTests(StageBatchSystemTestCase):
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
                result = create_starter_project(
                    InitRequest(template=template, output_dir=root)
                )

                self.assertEqual(result.template, template)
                self.assertEqual(result.output_dir, root.resolve())
                cfg_path = root / "experiment.yaml"
                self.assertEqual(result.config_path, cfg_path.resolve())
                self.assertTrue(cfg_path.exists())
                self.assertTrue((root / "README.sforge.md").exists())
                self.assertTrue((root / "CONFIG.sforge.md").exists())
                spec = load_experiment_spec(cfg_path)
                self.assertEqual(spec.stage_order(), expected_orders[template])

    def test_train_eval_config_fields_and_dry_run_full(self) -> None:
        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = root / "experiment.yaml"
            create_starter_project(InitRequest(template="train-eval", output_dir=root))

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
                payload["runtime"]["executor"]["python"]["bin"], "python3"
            )
            self.assertEqual(
                payload["runtime"]["user"]["default"]["python"]["bin"], "python3"
            )
            use_current_python_for_dry_run(cfg_path)

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

    def test_generated_scripts_are_model_integration_scaffolds(self) -> None:
        from slurmforge.starter import InitRequest, create_starter_project

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            create_starter_project(InitRequest(template="train-eval", output_dir=root))

            train_text = (root / "train.py").read_text(encoding="utf-8")
            eval_text = (root / "eval.py").read_text(encoding="utf-8")

            self.assertNotIn("__SFORGE_", train_text)
            self.assertNotIn("__SFORGE_", eval_text)
            for section in (
                "SECTION A - SlurmForge contract",
                "SECTION B - Your model code",
                "SECTION C - Output contract",
            ):
                self.assertIn(section, train_text)
                self.assertIn(section, eval_text)
            self.assertIn("def build_model", train_text)
            self.assertIn("def train_one_run", train_text)
            self.assertIn("checkpoints/**/*.pt", train_text)
            self.assertIn("def load_model_from_checkpoint", eval_text)
            self.assertIn("def evaluate", eval_text)
            self.assertIn("SFORGE_INPUT_CHECKPOINT", eval_text)
            self.assertIn("eval/metrics.json", eval_text)

    def test_script_template_renderer_rejects_placeholder_drift(self) -> None:
        from slurmforge.starter.errors import StarterTemplateError
        from slurmforge.starter.templates.script_render import (
            _render_asset,
            _validate_placeholders,
        )

        with self.assertRaisesRegex(StarterTemplateError, "undeclared placeholders"):
            _render_asset("train.py", {})
        with self.assertRaisesRegex(StarterTemplateError, "unused placeholders"):
            _validate_placeholders(
                "asset.py",
                "",
                {"__SFORGE_UNUSED__": "value"},
            )

    def test_all_templates_support_their_full_dry_run_command(self) -> None:
        from slurmforge.launcher import main
        from slurmforge.starter import InitRequest, create_starter_project

        templates = ("train-eval", "train-only", "eval-checkpoint")
        for template in templates:
            with self.subTest(template=template), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg_path = root / "experiment.yaml"
                create_starter_project(InitRequest(template=template, output_dir=root))
                use_current_python_for_dry_run(cfg_path)
                command = dry_run_command_for_template(template)
                args = [*command, "--config", str(cfg_path), "--dry-run=full"]

                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = main(args)

            self.assertEqual(code, 0, stderr.getvalue())
            self.assertEqual(json.loads(stdout.getvalue())["state"], "valid")
