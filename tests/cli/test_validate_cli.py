from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import io
from contextlib import redirect_stderr, redirect_stdout


class ValidateCliTests(StageBatchSystemTestCase):
    def test_validate_force_flag_is_removed(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        stderr = io.StringIO()
        with (
            redirect_stdout(io.StringIO()),
            redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            parser.parse_args(["validate", "--config", "experiment.yaml", "--force"])
        self.assertIn("unrecognized arguments: --force", stderr.getvalue())
