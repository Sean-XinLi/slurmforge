from __future__ import annotations

import unittest

from slurmforge.text_safety import slurm_safe_job_name


class TextSafetyTests(unittest.TestCase):
    def test_slurm_safe_job_name_replaces_unsafe_characters(self) -> None:
        self.assertEqual(
            slurm_safe_job_name('my "$(uname)" project / exp'),
            "my_uname_project_exp",
        )

    def test_slurm_safe_job_name_uses_default_when_empty_after_sanitization(self) -> None:
        self.assertEqual(slurm_safe_job_name('$$$///   '), "job")

    def test_slurm_safe_job_name_truncates_to_max_length(self) -> None:
        self.assertEqual(
            len(slurm_safe_job_name("x" * 256)),
            128,
        )

