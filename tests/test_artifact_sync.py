from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from slurmforge.execution import artifacts as artifact_sync


class ArtifactSyncTests(unittest.TestCase):
    def test_sync_patterns_records_copy_failures_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workdir = root / "work"
            result_dir = root / "result"
            workdir.mkdir()
            result_dir.mkdir()
            (workdir / "ok.txt").write_text("ok")
            (workdir / "fail.txt").write_text("fail")

            real_copy_item = artifact_sync.copy_item

            def fake_copy_item(src: Path, dst_root: Path, rel_root: Path) -> str:
                if src.name == "fail.txt":
                    raise PermissionError("denied")
                return real_copy_item(src, dst_root, rel_root)

            with patch("slurmforge.execution.artifacts.sync.copy_item", side_effect=fake_copy_item):
                copied, failures = artifact_sync.sync_patterns(
                    workdir=workdir,
                    result_dir=result_dir,
                    patterns=["*.txt"],
                    category="extra",
                    max_matches_per_glob=50,
                )

            self.assertEqual(len(copied), 1)
            self.assertTrue(copied[0].endswith("ok.txt"))
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]["stage"], "copy")
            self.assertEqual(failures[0]["source"], str((workdir / "fail.txt").resolve()))

    def test_sync_patterns_scans_multiple_workdirs_and_copies_each_unique_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            train_workdir = root / "train"
            eval_workdir = root / "eval"
            result_dir = root / "result"
            train_workdir.mkdir()
            eval_workdir.mkdir()
            result_dir.mkdir()
            (train_workdir / "shared.txt").write_text("train")
            (eval_workdir / "shared.txt").write_text("eval")
            (eval_workdir / "metrics.csv").write_text("1,2,3")

            copied, failures = artifact_sync.sync_patterns(
                workdir=[train_workdir, eval_workdir],
                result_dir=result_dir,
                patterns=["*.txt", "*.csv"],
                category="extra",
                max_matches_per_glob=50,
            )

            self.assertEqual(len(failures), 0)
            self.assertEqual(len(copied), 3)
            self.assertTrue(any(path.endswith("shared.txt") for path in copied))
            self.assertTrue(any(path.endswith("metrics.csv") for path in copied))

    def test_main_writes_manifest_and_exits_nonzero_on_partial_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workdir = root / "work"
            result_dir = root / "result"
            workdir.mkdir()
            (workdir / "ok.txt").write_text("ok")
            (workdir / "fail.txt").write_text("fail")

            real_copy_item = artifact_sync.copy_item

            def fake_copy_item(src: Path, dst_root: Path, rel_root: Path) -> str:
                if src.name == "fail.txt":
                    raise PermissionError("denied")
                return real_copy_item(src, dst_root, rel_root)

            args = Namespace(
                workdir=[str(workdir)],
                result_dir=str(result_dir),
                checkpoint_glob=[],
                eval_csv_glob=[],
                eval_image_glob=[],
                extra_glob=["*.txt"],
                max_matches_per_glob=50,
            )

            with patch("slurmforge.execution.artifacts.cli.parse_args", return_value=args):
                with patch("slurmforge.execution.artifacts.sync.copy_item", side_effect=fake_copy_item):
                    with self.assertRaises(SystemExit) as exc:
                        artifact_sync.main()

            self.assertEqual(exc.exception.code, 1)
            manifest = json.loads((result_dir / "meta" / "artifact_manifest.json").read_text())
            self.assertEqual(manifest["status"], "partial_failure")
            self.assertEqual(manifest["failure_count"], 1)
            self.assertEqual(len(manifest["copied"]["extra"]), 1)
            self.assertEqual(len(manifest["failures"]["extra"]), 1)
            self.assertEqual(manifest["workdirs"], [str(workdir.resolve())])
