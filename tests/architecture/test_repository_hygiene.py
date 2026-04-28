from __future__ import annotations

import subprocess
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class RepositoryHygieneTests(StageBatchSystemTestCase):
    def _tracked_files(self) -> list[str]:
        result = subprocess.run(
            ["git", "ls-files"],
            check=True,
            capture_output=True,
            text=True,
        )
        return [line for line in result.stdout.splitlines() if line]

    def test_tracked_files_do_not_include_local_artifacts(self) -> None:
        blocked_suffixes = (".pyc", ".swp", ".swo")
        blocked_names = {".DS_Store"}
        blocked_parts = {
            ".import_linter_cache",
            ".ruff_cache",
            ".venv",
            ".venv-fresh",
            "__pycache__",
        }
        violations: list[str] = []
        for tracked in self._tracked_files():
            path = Path(tracked)
            if not path.exists():
                continue
            parts = set(path.parts)
            if path.name in blocked_names:
                violations.append(tracked)
            elif tracked.endswith(blocked_suffixes):
                violations.append(tracked)
            elif path.name.endswith(".egg-info") or parts & blocked_parts:
                violations.append(tracked)
        self.assertEqual(violations, [])

    def test_import_linter_tooling_is_not_configured(self) -> None:
        checked_paths = [
            Path("pyproject.toml"),
            Path(".github/workflows/ci.yml"),
            Path("README.md"),
            Path("CONTRIBUTING.md"),
        ]
        text = "\n".join(path.read_text(encoding="utf-8") for path in checked_paths)
        self.assertFalse(Path(".importlinter").exists())
        self.assertNotIn("import-linter", text)
        self.assertNotIn("lint-imports", text)

    def test_ci_uses_current_tooling_contract(self) -> None:
        workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("ruff check src tests", workflow)
        self.assertIn("pytest -q", workflow)
        self.assertIn("python -m build", workflow)
        self.assertIn("python -m twine check dist/*", workflow)
        self.assertIn("needs: [lint, test]", workflow)
        self.assertEqual(workflow.count("python -m build"), 1)
        self.assertEqual(workflow.count("python -m twine check dist/*"), 1)

    def test_release_metadata_is_stable_1_0(self) -> None:
        pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
        citation = Path("CITATION.cff").read_text(encoding="utf-8")

        from slurmforge.identity import __version__

        self.assertEqual(__version__, "1.0.0")
        self.assertIn('version: "1.0.0"', citation)
        self.assertIn("Development Status :: 5 - Production/Stable", pyproject)
