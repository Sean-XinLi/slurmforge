from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.starter.helpers import init_args, interactive_init_args
import io
import tempfile
import yaml
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


class StarterTests(StageBatchSystemTestCase):
    def test_existing_files_are_not_overwritten_without_force_or_confirm(self) -> None:
        from slurmforge.starter import StarterWriteError
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            handle_init(init_args(root))
            original = (root / "experiment.yaml").read_text(encoding="utf-8")

            with (
                patch("sys.stdin.isatty", return_value=False),
                self.assertRaisesRegex(StarterWriteError, "--force"),
            ):
                handle_init(init_args(root))
            self.assertEqual(
                (root / "experiment.yaml").read_text(encoding="utf-8"), original
            )

    def test_interactive_selects_template_and_output(self) -> None:
        from slurmforge.cli.init import handle_init
        from slurmforge.spec import load_experiment_spec

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_dir = root / "custom_project"
            cfg_path = output_dir / "experiment.yaml"
            stdout = io.StringIO()
            with (
                patch("sys.stdin.isatty", return_value=True),
                patch("builtins.input", side_effect=["2", str(output_dir)]),
                redirect_stdout(stdout),
            ):
                handle_init(interactive_init_args())

            self.assertTrue(cfg_path.exists())
            spec = load_experiment_spec(cfg_path)
            self.assertEqual(spec.stage_order(), ("train",))
            self.assertIn("Select template:", stdout.getvalue())

    def test_interactive_confirm_allows_overwrite(self) -> None:
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = init_args(root)
            handle_init(args)
            (root / "experiment.yaml").write_text("changed: true\n", encoding="utf-8")

            with (
                patch("sys.stdin.isatty", return_value=True),
                patch("builtins.input", return_value="yes"),
            ):
                handle_init(args)

            payload = yaml.safe_load(
                (root / "experiment.yaml").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["project"], "demo")

    def test_interactive_cancel_preserves_existing_files(self) -> None:
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = init_args(root)
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
            self.assertEqual(
                (root / "experiment.yaml").read_text(encoding="utf-8"), original
            )

    def test_output_must_be_a_directory(self) -> None:
        from slurmforge.cli.init import handle_init
        from slurmforge.starter import StarterWriteError

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "not-a-directory"
            output.write_text("content\n", encoding="utf-8")

            with self.assertRaisesRegex(StarterWriteError, "not a directory"):
                handle_init(init_args(output))
