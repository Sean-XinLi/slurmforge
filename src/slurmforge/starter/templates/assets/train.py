from __future__ import annotations

import argparse
import os
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--lr", type=float, default=0.001)
    args = parser.parse_args()

    run_id = os.environ.get("SFORGE_RUN_ID", "local")
    checkpoint_dir = Path("checkpoints") / run_id
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_dir / f"step_{args.epochs:05d}.pt"
    checkpoint.write_text(f"run_id={run_id}\nlr={args.lr}\nepochs={args.epochs}\n", encoding="utf-8")

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    (log_dir / f"{run_id}.log").write_text(f"wrote {checkpoint}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
