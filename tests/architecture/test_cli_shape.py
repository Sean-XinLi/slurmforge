from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class CliShapeTests(StageBatchSystemTestCase):
    def test_cli_stage_common_is_split_by_concern(self) -> None:
        self.assertFalse(Path("src/slurmforge/cli/stage_common.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/args.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/builders.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/dry_run.py").exists())
        self.assertTrue(Path("src/slurmforge/cli/render.py").exists())

    def test_cli_flags_are_kebab_case_only(self) -> None:
        disallowed_flags = ("--dry_run", "--emit_only", "--project_root")
        checked = list(Path("src/slurmforge").rglob("*.py")) + [
            Path("README.md"),
            Path("docs/record-contract.md"),
        ]
        violations: list[str] = []
        for path in checked:
            text = path.read_text(encoding="utf-8")
            for flag in disallowed_flags:
                if flag in text:
                    violations.append(f"{path} contains {flag}")
        self.assertEqual(violations, [])

    def test_validate_cli_has_no_hidden_force_flag(self) -> None:
        validate_text = Path("src/slurmforge/cli/validate.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("--force", validate_text)
