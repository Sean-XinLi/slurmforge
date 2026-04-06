#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from ..pipeline.train_outputs import write_train_outputs_contract


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Low-level runtime helper that resolves train outputs for eval handoff. "
            "This command is usually invoked by generated batch scripts."
        )
    )
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--manifest_path", required=True)
    parser.add_argument("--env_path", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--model_name", required=True)
    parser.add_argument("--workdir", action="append", default=[])
    parser.add_argument("--checkpoint_glob", action="append", default=[])
    parser.add_argument("--primary_policy", choices=["latest", "best", "explicit"], default="latest")
    parser.add_argument("--explicit_checkpoint", default="")
    parser.add_argument("--max_matches_per_glob", type=int, default=500)
    parser.add_argument("--require_primary", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = write_train_outputs_contract(
        result_dir=Path(args.result_dir),
        manifest_path=Path(args.manifest_path),
        env_path=Path(args.env_path),
        checkpoint_globs=list(args.checkpoint_glob or []),
        run_id=str(args.run_id),
        model_name=str(args.model_name),
        primary_policy=str(args.primary_policy),
        explicit_checkpoint=str(args.explicit_checkpoint or "").strip() or None,
        workdirs=list(args.workdir or []),
        max_matches_per_glob=max(1, int(args.max_matches_per_glob)),
    )
    print(f"[train_outputs] wrote manifest: {args.manifest_path}")
    if manifest.primary_checkpoint:
        print(f"[train_outputs] primary_checkpoint={manifest.primary_checkpoint}")
    else:
        print(f"[train_outputs][WARN] primary checkpoint unresolved status={manifest.status}")
        if manifest.selection_error:
            print(f"[train_outputs][WARN] selection_error={manifest.selection_error}")
        if args.require_primary:
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
