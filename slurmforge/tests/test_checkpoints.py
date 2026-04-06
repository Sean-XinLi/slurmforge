from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from slurmforge.pipeline.checkpoints import (
    checkpoint_state_path_for_result_dir,
    read_checkpoint_state,
    select_checkpoint_state,
    write_checkpoint_state,
)


class CheckpointStateTests(unittest.TestCase):
    def test_select_checkpoint_state_prefers_highest_step_over_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "job-101"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            lower = checkpoint_dir / "step-2.ckpt"
            higher = checkpoint_dir / "step-10.ckpt"
            lower.write_text("low", encoding="utf-8")
            higher.write_text("high", encoding="utf-8")
            os.utime(lower, (2_000_000_000, 2_000_000_000))
            os.utime(higher, (1_000_000_000, 1_000_000_000))

            state = select_checkpoint_state(result_dir, ())

        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.selection_reason, "max_step")
        self.assertEqual(state.global_step, 10)
        self.assertEqual(state.latest_checkpoint_rel, "checkpoints/step-10.ckpt")

    def test_select_checkpoint_state_rejects_ambiguous_candidates_without_step_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "job-102"
            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            (checkpoint_dir / "latest.ckpt").write_text("a", encoding="utf-8")
            (checkpoint_dir / "backup.ckpt").write_text("b", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Unable to deterministically select a checkpoint"):
                select_checkpoint_state(result_dir, ())

    def test_checkpoint_state_roundtrip_uses_result_dir_relative_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp) / "job-103"
            result_dir.mkdir(parents=True, exist_ok=True)
            state = select_checkpoint_state(result_dir, ())
            self.assertIsNone(state)

            checkpoint_dir = result_dir / "checkpoints"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            (checkpoint_dir / "step-5.ckpt").write_text("c", encoding="utf-8")
            state = select_checkpoint_state(result_dir, ())
            assert state is not None
            write_checkpoint_state(result_dir, state)
            restored = read_checkpoint_state(result_dir)

        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.latest_checkpoint_rel, "checkpoints/step-5.ckpt")
        self.assertTrue(checkpoint_state_path_for_result_dir(result_dir).name.endswith("checkpoint_state.json"))
