#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Mapping

from ..pipeline.status import (
    attempt_result_path_for_result_dir,
    build_attempt_result,
    write_attempt_result,
)


def write_attempt_result_for_result_dir(
    *,
    result_dir: Path,
    train_exit_code: int,
    eval_exit_code: int,
    env: Mapping[str, str] | None = None,
) -> Path:
    resolved_result_dir = result_dir.resolve()
    (resolved_result_dir / "meta").mkdir(parents=True, exist_ok=True)
    attempt = build_attempt_result(
        result_dir=resolved_result_dir,
        train_exit_code=train_exit_code,
        eval_exit_code=eval_exit_code,
        env=env,
    )
    path = attempt_result_path_for_result_dir(resolved_result_dir)
    write_attempt_result(path, attempt)
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Low-level runtime helper that persists structured attempt_result.json "
            "for one executed slurmforge run."
        )
    )
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--train_exit_code", required=True, type=int)
    parser.add_argument("--eval_exit_code", required=True, type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = write_attempt_result_for_result_dir(
        result_dir=Path(args.result_dir),
        train_exit_code=int(args.train_exit_code),
        eval_exit_code=int(args.eval_exit_code),
    )
    print(f"[attempt_result] wrote {path}")


if __name__ == "__main__":
    main()
