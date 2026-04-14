#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Sequence

from .sync import sync_artifacts


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Low-level runtime helper that syncs training and eval artifacts into a result folder. "
            "This command is usually invoked by generated batch scripts or debugging workflows."
        )
    )
    parser.add_argument("--workdir", action="append", required=True)
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--checkpoint_glob", action="append", default=[])
    parser.add_argument("--eval_csv_glob", action="append", default=[])
    parser.add_argument("--eval_image_glob", action="append", default=[])
    parser.add_argument("--extra_glob", action="append", default=[])
    parser.add_argument("--max_matches_per_glob", type=int, default=500)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    result_dir = Path(args.result_dir)
    summary = sync_artifacts(
        workdir=args.workdir,
        result_dir=result_dir,
        category_patterns={
            "checkpoints": list(args.checkpoint_glob or []),
            "eval_csv": list(args.eval_csv_glob or []),
            "eval_images": list(args.eval_image_glob or []),
            "extra": list(args.extra_glob or []),
        },
        max_matches_per_glob=max(1, int(args.max_matches_per_glob)),
        warn_prefix="artifact_sync",
    )

    batch_root_env = (os.environ.get("AI_INFRA_BATCH_ROOT") or "").strip()
    if batch_root_env:
        from ...storage import open_batch_storage
        handle = open_batch_storage(Path(batch_root_env))
        handle.execution.write_artifact_manifest(result_dir.resolve(), summary)

    summary_path = result_dir.resolve() / "meta" / "artifact_manifest.json"
    print(f"[artifact_sync] wrote manifest: {summary_path}")
    failure_count = int(summary["failure_count"])
    if failure_count:
        print(f"[artifact_sync][WARN] completed with {failure_count} artifact sync failure(s)")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
