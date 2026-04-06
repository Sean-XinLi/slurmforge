from __future__ import annotations

import unittest

from slurmforge.pipeline.config.normalize.slurm_deps import (
    normalize_dependency_kind,
    normalize_dependency_mapping,
)
from slurmforge.pipeline.materialization.slurm_deps import (
    build_sbatch_dependency_flag,
)


class SlurmDependenciesTests(unittest.TestCase):
    def test_normalize_dependency_kind_accepts_supported_value(self) -> None:
        self.assertEqual(normalize_dependency_kind("AFTEROK", field_name="notify.when"), "afterok")

    def test_normalize_dependency_kind_rejects_shellish_strings(self) -> None:
        for raw_value in ("after ok", "afterok; rm -rf /", "afterok | cat", "afterok $(uname)"):
            with self.subTest(raw_value=raw_value):
                with self.assertRaisesRegex(ValueError, "notify\\.when must be one of"):
                    normalize_dependency_kind(raw_value, field_name="notify.when")

    def test_normalize_dependency_mapping_rejects_empty_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "output\\.dependencies\\.afterok must contain only non-empty values"):
            normalize_dependency_mapping(
                {"afterok": ["101", ""]},
                field_name="output.dependencies",
            )

    def test_normalize_dependency_mapping_deduplicates_while_preserving_order(self) -> None:
        normalized = normalize_dependency_mapping(
            {
                "afterok": ["101", "202", "101", "202", "303"],
                "afterany": ["9", "9", "10"],
            },
            field_name="output.dependencies",
        )
        self.assertEqual(
            normalized,
            {
                "afterany": ["9", "10"],
                "afterok": ["101", "202", "303"],
            },
        )

    def test_build_sbatch_dependency_flag_orders_kinds_and_formats_values(self) -> None:
        flag = build_sbatch_dependency_flag(
            {
                "afterok": ["101", "202"],
                "after": ["9"],
                "afternotok": ["999"],
            }
        )
        self.assertEqual(flag, "--dependency=after:9,afterok:101:202,afternotok:999 ")

    def test_build_sbatch_dependency_flag_ignores_empty_sections(self) -> None:
        self.assertEqual(build_sbatch_dependency_flag({}), "")

