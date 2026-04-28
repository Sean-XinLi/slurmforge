from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class EvalContext:
    run_id: str
    stage_name: str
    attempt_dir: Path
    metrics_dir: Path
    injected_checkpoint: str

    @classmethod
    def from_env(cls) -> "EvalContext":
        return cls(
            run_id=os.environ.get("SFORGE_RUN_ID", "local"),
            stage_name=os.environ.get("SFORGE_STAGE_NAME", "eval"),
            attempt_dir=Path(os.environ.get("SFORGE_ATTEMPT_DIR", ".")).resolve(),
            metrics_dir=Path("eval"),
            injected_checkpoint=os.environ.get("SFORGE_INPUT_CHECKPOINT", ""),
        )


# SECTION A - SlurmForge contract. Keep checkpoint_path and add eval args here.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--split", default="validation")
    return parser.parse_args()


def resolve_checkpoint(args: argparse.Namespace, context: EvalContext) -> Path:
    checkpoint = Path(args.checkpoint_path)
    if not checkpoint.exists():
        raise FileNotFoundError(f"checkpoint does not exist: {checkpoint}")
    if context.injected_checkpoint:
        injected = Path(context.injected_checkpoint)
        if injected.resolve() != checkpoint.resolve():
            raise RuntimeError(
                "checkpoint_path does not match SFORGE_INPUT_CHECKPOINT; "
                "keep stages.eval.inputs.checkpoint.inject flag/env aligned"
            )
    return checkpoint


# SECTION B - Your model code. Replace these demo functions with real eval.
def load_model_from_checkpoint(checkpoint: Path) -> dict[str, Any]:
    try:
        return json.loads(checkpoint.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"checkpoint": str(checkpoint)}


def load_eval_data(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "split": args.split,
    }


def evaluate(
    args: argparse.Namespace,
    context: EvalContext,
    checkpoint: Path,
    model: dict[str, Any],
    dataset: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": context.run_id,
        "stage_name": context.stage_name,
        "split": dataset["split"],
        "checkpoint": str(checkpoint),
        "accuracy": 1.0,
        "model": model.get("model", model.get("checkpoint", "unknown")),
    }


# SECTION C - Output contract. SlurmForge reads eval/metrics.json $.accuracy.
def write_metrics(metrics: dict[str, Any], context: EvalContext) -> Path:
    context.metrics_dir.mkdir(parents=True, exist_ok=True)
    path = context.metrics_dir / "metrics.json"
    path.write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def require_metrics_contract(metrics_path: Path) -> None:
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    value = metrics.get("accuracy")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(
            "eval/metrics.json must contain a numeric accuracy field "
            "to match stages.eval.outputs.accuracy.json_path"
        )


def main() -> None:
    args = parse_args()
    context = EvalContext.from_env()
    checkpoint = resolve_checkpoint(args, context)
    model = load_model_from_checkpoint(checkpoint)
    dataset = load_eval_data(args)
    metrics = evaluate(args, context, checkpoint, model, dataset)
    metrics_path = write_metrics(metrics, context)
    require_metrics_contract(metrics_path)


if __name__ == "__main__":
    main()
