from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.std import Namespace, Path, io, patch, redirect_stdout, tempfile, yaml

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
    def test_existing_files_are_not_overwritten_without_force_or_confirm(self) -> None:
        from slurmforge.starter import StarterWriteError
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            handle_init(self._init_args(root))
            original = (root / "experiment.yaml").read_text(encoding="utf-8")

            with patch("sys.stdin.isatty", return_value=False), self.assertRaisesRegex(StarterWriteError, "--force"):
                handle_init(self._init_args(root))
            self.assertEqual((root / "experiment.yaml").read_text(encoding="utf-8"), original)
    def test_interactive_selects_template_and_output(self) -> None:
        from slurmforge.cli.init import handle_init
        from slurmforge.spec import load_experiment_spec

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = root / "custom.yaml"
            stdout = io.StringIO()
            with (
                patch("sys.stdin.isatty", return_value=True),
                patch("builtins.input", side_effect=["2", str(cfg_path)]),
                redirect_stdout(stdout),
            ):
                handle_init(self._interactive_init_args())

            self.assertTrue(cfg_path.exists())
            spec = load_experiment_spec(cfg_path)
            self.assertEqual(spec.stage_order(), ("train",))
            self.assertIn("Select template:", stdout.getvalue())
    def test_interactive_confirm_allows_overwrite(self) -> None:
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self._init_args(root)
            handle_init(args)
            (root / "experiment.yaml").write_text("changed: true\n", encoding="utf-8")

            with (
                patch("sys.stdin.isatty", return_value=True),
                patch("builtins.input", return_value="yes"),
            ):
                handle_init(args)

            payload = yaml.safe_load((root / "experiment.yaml").read_text(encoding="utf-8"))
            self.assertEqual(payload["project"], "demo")
    def test_interactive_cancel_preserves_existing_files(self) -> None:
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self._init_args(root)
            handle_init(args)
            original = (root / "experiment.yaml").read_text(encoding="utf-8")
            stdout = io.StringIO()
            with (
                patch("sys.stdin.isatty", return_value=True),
                patch("builtins.input", return_value="n"),
                redirect_stdout(stdout),
            ):
                handle_init(args)

            self.assertIn("[INIT] cancelled", stdout.getvalue())
            self.assertEqual((root / "experiment.yaml").read_text(encoding="utf-8"), original)



def _dry_run_command_for_template(template: str, _root: Path) -> list[str]:
    if template == "train-eval":
        return ["run"]
    if template == "train-only":
        return ["train"]
    return ["eval", "--checkpoint", "checkpoint.pt"]


def _bad_template(file_builder):
    from slurmforge.starter.models import StarterCommandSet, StarterReadmePlan, StarterTemplate

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
