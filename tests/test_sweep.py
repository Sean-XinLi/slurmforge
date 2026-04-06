from __future__ import annotations

import unittest


from slurmforge.sweep import count_sweep, iter_sweep


class SweepTests(unittest.TestCase):
    def test_iter_sweep_uses_max_runs_limit(self) -> None:
        cfg = {
            "sweep": {
                "enabled": True,
                "max_runs": 3,
                "shared_axes": {
                    "run.args.lr": [1, 2, 3],
                    "run.args.bs": [16, 32, 64],
                },
            }
        }
        runs = list(iter_sweep(cfg))
        self.assertEqual(len(runs), 3)

    def test_iter_sweep_does_not_mutate_base_cfg(self) -> None:
        cfg = {
            "run": {"args": {"lr": 0.001, "bs": 32}},
            "sweep": {
                "enabled": True,
                "shared_axes": {
                    "run.args.lr": [0.1, 0.2],
                },
            },
        }
        _runs = list(iter_sweep(cfg))
        self.assertEqual(cfg["run"]["args"]["lr"], 0.001)

    def test_iter_sweep_shared_axes_count_and_order_are_stable(self) -> None:
        cfg = {
            "sweep": {
                "enabled": True,
                "max_runs": 3,
                "shared_axes": {
                    "run.args.z": [1, 2],
                    "run.args.a": ["x", "y"],
                },
            }
        }
        self.assertEqual(count_sweep(cfg), 3)
        runs = list(iter_sweep(cfg))
        self.assertEqual(len(runs), 3)
        self.assertEqual(runs[0]["run"]["args"]["a"], "x")
        self.assertEqual(runs[0]["run"]["args"]["z"], 1)
        self.assertEqual(runs[1]["run"]["args"]["a"], "x")
        self.assertEqual(runs[1]["run"]["args"]["z"], 2)
        self.assertEqual(runs[2]["run"]["args"]["a"], "y")
        self.assertEqual(runs[2]["run"]["args"]["z"], 1)

    def test_iter_sweep_cases_without_shared_axes_are_supported(self) -> None:
        cfg = {
            "sweep": {
                "enabled": True,
                "cases": [
                    {"name": "baseline", "set": {"run.args.ctw_scope": "none"}},
                    {"name": "chrom6", "set": {"run.args.ctw_scope": "chrom", "run.args.depth": 6}},
                ],
            }
        }
        runs = list(iter_sweep(cfg))
        self.assertEqual(len(runs), 2)
        self.assertEqual(runs[0]["run"]["args"]["ctw_scope"], "none")
        self.assertEqual(runs[1]["run"]["args"]["ctw_scope"], "chrom")
        self.assertEqual(runs[1]["run"]["args"]["depth"], 6)

    def test_iter_sweep_case_local_axes_do_not_repeat_baseline(self) -> None:
        cfg = {
            "sweep": {
                "enabled": True,
                "cases": [
                    {"name": "baseline", "set": {"run.args.ctw_scope": "none"}},
                    {
                        "name": "chrom",
                        "set": {"run.args.ctw_scope": "chrom"},
                        "axes": {"run.args.depth": [6, 8]},
                    },
                    {
                        "name": "global",
                        "set": {"run.args.ctw_scope": "global"},
                        "axes": {"run.args.depth": [6, 8]},
                    },
                ],
            }
        }
        runs = list(iter_sweep(cfg))
        signatures = [
            (run["run"]["args"]["ctw_scope"], run["run"]["args"].get("depth"))
            for run in runs
        ]
        self.assertEqual(
            signatures,
            [
                ("none", None),
                ("chrom", 6),
                ("chrom", 8),
                ("global", 6),
                ("global", 8),
            ],
        )

    def test_iter_sweep_hybrid_shared_and_case_axes_expand_together(self) -> None:
        cfg = {
            "sweep": {
                "enabled": True,
                "shared_axes": {"run.args.lr": [1, 2]},
                "cases": [
                    {"name": "baseline", "set": {"run.args.ctw_scope": "none"}},
                    {
                        "name": "chrom",
                        "set": {"run.args.ctw_scope": "chrom"},
                        "axes": {"run.args.depth": [6, 8]},
                    },
                ],
            }
        }
        self.assertEqual(count_sweep(cfg), 6)
        runs = list(iter_sweep(cfg))
        signatures = [
            (
                run["run"]["args"]["ctw_scope"],
                run["run"]["args"].get("depth"),
                run["run"]["args"]["lr"],
            )
            for run in runs
        ]
        self.assertEqual(
            signatures,
            [
                ("none", None, 1),
                ("none", None, 2),
                ("chrom", 6, 1),
                ("chrom", 6, 2),
                ("chrom", 8, 1),
                ("chrom", 8, 2),
            ],
        )

    def test_iter_sweep_rejects_overlapping_case_paths(self) -> None:
        cfg = {
            "sweep": {
                "enabled": True,
                "cases": [
                    {
                        "name": "bad",
                        "set": {"run.args": {"lr": 1}},
                        "axes": {"run.args.lr": [1, 2]},
                    }
                ],
            }
        }
        with self.assertRaisesRegex(ValueError, "overlapping override paths"):
            list(iter_sweep(cfg))
