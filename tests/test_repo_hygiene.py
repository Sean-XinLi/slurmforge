from __future__ import annotations

import unittest
from pathlib import Path

from tests._support import list_generated_artifact_dirs, remove_generated_artifact_dirs


class RepoHygieneTests(unittest.TestCase):
    def test_repo_cleanup_policy_removes_generated_artifact_directories(self) -> None:
        root = Path(__file__).resolve().parents[1]
        remove_generated_artifact_dirs(root)
        unexpected = [str(path.relative_to(root)) for path in list_generated_artifact_dirs(root)]

        self.assertEqual(unexpected, [], msg="generated artifact directories must not live in the product source tree")
