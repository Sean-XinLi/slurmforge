from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CHECKPOINT_FLAG = "__SFORGE_CHECKPOINT_FLAG__"
CHECKPOINT_ARG_DEST = CHECKPOINT_FLAG.lstrip("-").replace("-", "_").replace(".", "_")
CHECKPOINT_ENV = "__SFORGE_CHECKPOINT_ENV__"
ACCURACY_FILE = Path("__SFORGE_ACCURACY_FILE__")
ACCURACY_FIELD = "__SFORGE_ACCURACY_FIELD__"
EVAL_SPLIT_DEFAULT = "__SFORGE_EVAL_SPLIT_DEFAULT__"


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
            metrics_dir=ACCURACY_FILE.parent,
            injected_checkpoint=os.environ.get(CHECKPOINT_ENV, ""),
        )


# SECTION A - SlurmForge contract. Keep injected checkpoint args here.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        _cli_flag(CHECKPOINT_FLAG),
        dest=CHECKPOINT_ARG_DEST,
        required=True,
    )
    parser.add_argument("--split", default=EVAL_SPLIT_DEFAULT)
    return parser.parse_args()


def resolve_checkpoint(args: argparse.Namespace, context: EvalContext) -> Path:
    checkpoint = Path(getattr(args, CHECKPOINT_ARG_DEST))
    if not checkpoint.exists():
        raise FileNotFoundError(f"checkpoint does not exist: {checkpoint}")
    if context.injected_checkpoint:
        injected = Path(context.injected_checkpoint)
        if injected.resolve() != checkpoint.resolve():
            raise RuntimeError(
                f"{_cli_flag(CHECKPOINT_FLAG)} does not match {CHECKPOINT_ENV}; "
                "keep stages.eval.inputs.checkpoint.inject aligned"
            )
    return checkpoint


def _cli_flag(name: str) -> str:
    if name.startswith("-"):
        return name
    return f"--{name}"


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
        ACCURACY_FIELD: 1.0,
        "model": model.get("model", model.get("checkpoint", "unknown")),
    }


# SECTION C - Output contract.
# SlurmForge reads __SFORGE_ACCURACY_FILE__ __SFORGE_ACCURACY_JSON_PATH__.
def write_metrics(metrics: dict[str, Any], context: EvalContext) -> Path:
    context.metrics_dir.mkdir(parents=True, exist_ok=True)
    path = context.metrics_dir / ACCURACY_FILE.name
    path.write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def require_metrics_contract(metrics_path: Path) -> None:
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    value = metrics.get(ACCURACY_FIELD)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(
            f"{ACCURACY_FILE} must contain a numeric {ACCURACY_FIELD} field "
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
