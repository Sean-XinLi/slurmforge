from __future__ import annotations

import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from slurmforge.cli.init import _default_out_dir, _prompt_overwrite


class DefaultOutDirTests(unittest.TestCase):
    def test_derives_name_from_template_type(self) -> None:
        for ttype in ("script", "command", "registry", "adapter"):
            with self.subTest(ttype=ttype):
                self.assertEqual(_default_out_dir(ttype), Path(f"./slurmforge_{ttype}_starter"))


class PromptOverwriteTests(unittest.TestCase):
    def test_returns_true_for_nonexistent_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(_prompt_overwrite(Path(tmp) / "does_not_exist"))

    def test_returns_true_for_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            empty = Path(tmp) / "empty"
            empty.mkdir()
            self.assertTrue(_prompt_overwrite(empty))

    def test_non_tty_non_empty_exits_with_code_1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            non_empty = Path(tmp) / "project"
            non_empty.mkdir()
            (non_empty / "experiment.yaml").write_text("foo: bar\n", encoding="utf-8")

            with patch.object(sys.stdin, "isatty", return_value=False), \
                 patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit) as ctx:
                    _prompt_overwrite(non_empty)

            self.assertEqual(ctx.exception.code, 1)
            self.assertIn("--force", mock_stderr.getvalue())

    def test_non_tty_non_empty_error_message_names_the_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            non_empty = Path(tmp) / "my_project"
            non_empty.mkdir()
            (non_empty / "experiment.yaml").write_text("foo: bar\n", encoding="utf-8")

            with patch.object(sys.stdin, "isatty", return_value=False), \
                 patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with self.assertRaises(SystemExit):
                    _prompt_overwrite(non_empty)

            self.assertIn("my_project", mock_stderr.getvalue())
