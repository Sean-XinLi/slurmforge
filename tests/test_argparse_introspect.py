from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


from slurmforge.model_support.argparse_introspect import extract_cli_arg_actions


class ArgparseIntrospectTests(unittest.TestCase):
    def test_argparse_introspection_records_canonical_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "train.py"
            script.write_text(
                "\n".join(
                    [
                        "import argparse",
                        "p = argparse.ArgumentParser()",
                        "p.add_argument('--learning-rate', type=float)",
                        "p.add_argument('--use_amp', action='store_true')",
                    ]
                ),
                encoding="utf-8",
            )
            actions = extract_cli_arg_actions(str(script))
        self.assertEqual(actions.get("learning_rate"), "value:--learning-rate")
        self.assertEqual(actions.get("use_amp"), "store_true:--use_amp")
