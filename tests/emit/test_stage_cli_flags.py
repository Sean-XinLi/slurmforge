from __future__ import annotations

from pathlib import Path
import tempfile

import yaml

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    build_shell_script,
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)


class StageCliFlagTests(StageBatchSystemTestCase):
    def test_python_script_shell_keeps_entry_arg_flag_names_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
            payload["stages"]["eval"]["enabled"] = False
            payload["stages"]["train"]["entry"]["args"] = {
                "max_length": 1024,
                "max-length": 2048,
                "--raw.flag": True,
                "disabled": False,
                "layers": [2, 4],
                "skip": None,
            }
            cfg_path.write_text(
                yaml.safe_dump(payload, sort_keys=False), encoding="utf-8"
            )

            batch = compile_stage_batch_for_kind(
                load_experiment_spec(cfg_path), kind="train"
            )
            shell = build_shell_script(batch.stage_instances[0], ())

            self.assertIn("--max_length 1024", shell)
            self.assertIn("--max-length 2048", shell)
            self.assertIn("--raw.flag", shell)
            self.assertIn("--disabled false", shell)
            self.assertEqual(shell.count("--layers"), 2)
            self.assertNotIn("--max-length 1024", shell)
            self.assertNotIn("--skip", shell)

    def test_python_script_shell_uses_same_flag_rules_for_input_injection(self) -> None:
        from slurmforge.contracts import (
            InputBinding,
            InputInjection,
            InputSource,
            ResolvedInput,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
            payload["stages"]["eval"]["enabled"] = False
            cfg_path.write_text(
                yaml.safe_dump(payload, sort_keys=False), encoding="utf-8"
            )

            batch = compile_stage_batch_for_kind(
                load_experiment_spec(cfg_path), kind="train"
            )
            bindings = (
                InputBinding(
                    input_name="checkpoint",
                    source=InputSource(kind="external_path", path="/tmp/checkpoint.pt"),
                    expects="path",
                    required=True,
                    resolved=ResolvedInput(kind="path", path="/tmp/checkpoint.pt"),
                    inject=InputInjection(flag="checkpoint_path", mode="path"),
                ),
                InputBinding(
                    input_name="raw",
                    source=InputSource(kind="external_path", path="/tmp/raw.pt"),
                    expects="path",
                    resolved=ResolvedInput(kind="path", path="/tmp/raw.pt"),
                    inject=InputInjection(flag="--raw.input", mode="path"),
                ),
            )

            shell = build_shell_script(batch.stage_instances[0], bindings)

            self.assertIn("--checkpoint_path /tmp/checkpoint.pt", shell)
            self.assertNotIn("--checkpoint-path", shell)
            self.assertIn("--raw.input /tmp/raw.pt", shell)
            self.assertNotIn("----raw.input", shell)
