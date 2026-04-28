from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.starter.helpers import init_args
import io
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class StarterTests(StageBatchSystemTestCase):
    def test_starter_facade_exports_only_stable_api(self) -> None:
        import slurmforge.starter as starter

        for name in (
            "FilePayload",
            "RenderedFile",
            "StarterWritePlan",
            "StarterTemplate",
            "get_template",
        ):
            self.assertFalse(hasattr(starter, name), name)

    def test_lists_templates_without_writing_files(self) -> None:
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = init_args(root)
            args.list_templates = True
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_init(args)
            self.assertIn("train-eval:", stdout.getvalue())
            self.assertIn("eval-checkpoint:", stdout.getvalue())
            self.assertFalse((root / "experiment.yaml").exists())

    def test_init_rejects_yaml_detail_flags(self) -> None:
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
