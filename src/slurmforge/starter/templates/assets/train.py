from __future__ import annotations

import argparse
import json
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TrainContext:
    run_id: str
    stage_name: str
    attempt_dir: Path
    checkpoint_dir: Path
    log_dir: Path

    @classmethod
    def from_env(cls) -> "TrainContext":
        run_id = os.environ.get("SFORGE_RUN_ID", "local")
        return cls(
            run_id=run_id,
            stage_name=os.environ.get("SFORGE_STAGE_NAME", "train"),
            attempt_dir=Path(os.environ.get("SFORGE_ATTEMPT_DIR", ".")).resolve(),
            checkpoint_dir=Path("checkpoints") / run_id,
            log_dir=Path("logs"),
        )


# SECTION A - SlurmForge contract. Keep this shape and add your own args here.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="baseline")
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--lr", type=float, default=0.001)
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--seed", type=int, default=1)
    return parser.parse_args()


# SECTION B - Your model code. Replace these demo functions with real training.
def build_model(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "name": args.model,
        "seed": args.seed,
    }


def load_training_data(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "batch_size": args.batch_size,
        "num_examples": args.batch_size * max(args.epochs, 1),
    }


def train_one_run(
    args: argparse.Namespace,
    context: TrainContext,
    model: dict[str, Any],
    dataset: dict[str, Any],
) -> Path:
    random.seed(args.seed)
    context.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    context.log_dir.mkdir(parents=True, exist_ok=True)

    checkpoint = context.checkpoint_dir / f"step_{args.epochs:05d}.pt"
    checkpoint_payload = {
        "format": "slurmforge-demo-checkpoint",
        "run_id": context.run_id,
        "stage_name": context.stage_name,
        "model": model,
        "dataset": dataset,
        "hyperparameters": {
            "epochs": args.epochs,
            "lr": args.lr,
            "batch_size": args.batch_size,
            "seed": args.seed,
        },
    }
    checkpoint.write_text(
        json.dumps(checkpoint_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_train_summary(args, context, checkpoint)
    return checkpoint


def write_train_summary(
    args: argparse.Namespace,
    context: TrainContext,
    checkpoint: Path,
) -> None:
    summary = {
        "run_id": context.run_id,
        "stage_name": context.stage_name,
        "checkpoint": str(checkpoint),
        "epochs": args.epochs,
        "lr": args.lr,
        "batch_size": args.batch_size,
        "seed": args.seed,
    }
    (context.log_dir / f"{context.run_id}.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


# SECTION C - Output contract. SlurmForge discovers checkpoints/**/*.pt.
def require_checkpoint_contract(checkpoint: Path) -> None:
    if not checkpoint.exists():
        raise RuntimeError(f"training did not create checkpoint: {checkpoint}")
    if "checkpoints" not in checkpoint.parts or checkpoint.suffix != ".pt":
        raise RuntimeError(
            "training checkpoint must be a .pt file under checkpoints/ "
            "to match stages.train.outputs.checkpoint.discover.globs"
        )


def main() -> None:
    args = parse_args()
    context = TrainContext.from_env()
    model = build_model(args)
    dataset = load_training_data(args)
    checkpoint = train_one_run(args, context, model, dataset)
    require_checkpoint_contract(checkpoint)


if __name__ == "__main__":
    main()
