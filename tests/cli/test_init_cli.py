from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import io
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class InitCliTests(StageBatchSystemTestCase):
    def test_init_cli_rejects_yaml_detail_flags(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        stderr = io.StringIO()
        with (
            redirect_stdout(io.StringIO()),
            redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            parser.parse_args(["init", "--project", "demo"])
        self.assertIn("unrecognized arguments: --project", stderr.getvalue())

    def test_init_help_only_exposes_scaffolding_options(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        stdout = io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            parser.parse_args(["init", "--help"])

        self.assertEqual(raised.exception.code, 0)
        help_text = stdout.getvalue()
        for option in ("--list-templates", "--template", "--output", "--force"):
            self.assertIn(option, help_text)
        for disallowed in (
            "--project",
            "--experiment",
            "--storage-root",
            "--partition",
            "--python-bin",
        ):
            self.assertNotIn(disallowed, help_text)

    def test_user_cli_errors_do_not_emit_traceback(self) -> None:
        from slurmforge.launcher import main

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(
                main(["init", "--template", "train-eval", "--output", str(root)]),
                0,
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = main(
                    ["init", "--template", "train-eval", "--output", str(root)]
                )

            combined = stdout.getvalue() + stderr.getvalue()
            self.assertEqual(code, 2)
            self.assertIn(
                "[ERROR] Refusing to overwrite existing files:", stderr.getvalue()
            )
            self.assertNotIn("Traceback", combined)
