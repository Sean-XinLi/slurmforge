from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch


class RootCliTests(StageBatchSystemTestCase):
    def test_unknown_cli_bugs_are_not_caught(self) -> None:
        import slurmforge.launcher as launcher

        def raise_bug(_args) -> None:
            raise RuntimeError("internal bug")

        parser = launcher.build_parser()
        parsed = Namespace(handler=raise_bug)
        with (
            patch.object(parser, "parse_args", return_value=parsed),
            patch.object(launcher, "build_parser", return_value=parser),
            self.assertRaisesRegex(RuntimeError, "internal bug"),
        ):
            launcher.main([])

    def test_root_cli_exposes_expected_commands(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        subparser_action = next(
            action for action in parser._actions if getattr(action, "choices", None)
        )
        self.assertEqual(
            set(subparser_action.choices),
            {
                "init",
                "validate",
                "estimate",
                "plan",
                "train",
                "eval",
                "run",
                "status",
                "resubmit",
                "pipeline",
            },
        )
        pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('sforge = "slurmforge.launcher:main"', pyproject)
        self.assertNotIn("sforge-stage-executor", pyproject)
