from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase


class CliFlagRenderingTests(StageBatchSystemTestCase):
    def test_entry_args_keys_render_as_cli_flags_exactly_as_configured(self) -> None:
        from slurmforge.executor.launcher.args import args_to_argv

        self.assertEqual(
            args_to_argv(
                {
                    "max_length": 1024,
                    "max-length": 2048,
                    "--raw.flag": True,
                    "disabled": False,
                    "layers": [2, 4],
                    "skip": None,
                }
            ),
            [
                "--raw.flag",
                "--disabled",
                "false",
                "--layers",
                "2",
                "--layers",
                "4",
                "--max-length",
                "2048",
                "--max_length",
                "1024",
            ],
        )
