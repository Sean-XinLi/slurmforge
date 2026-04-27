from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from ..io import to_jsonable
from ..orchestration import (
    ExecutionMode,
    build_eval_stage_batch,
    build_dry_run_audit,
    build_train_eval_pipeline_plan,
    build_train_stage_batch,
    resolve_eval_inputs,
    summarize_train_eval_pipeline_plan,
    summarize_stage_batch,
)
from ..spec import ExperimentSpec, load_experiment_spec
from .requests import EvalInputSourceRequest, eval_source_from_args


def add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", required=True, help="Path to stage-batch experiment YAML")
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        help="Override config by dot-path, e.g. --set stages.train.entry.args.lr=0.004",
    )
    parser.add_argument(
        "--project-root",
        dest="project_root",
        default=None,
        help="Override project root used to resolve relative paths (default: config file directory)",
    )


def add_execution_mode_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        nargs="?",
        const="summary",
        default=False,
        choices=("summary", "json", "full"),
        help="Compile without writing files. Use --dry-run=json or --dry-run=full for machine-readable audit output",
    )
    parser.add_argument(
        "--emit-only",
        dest="emit_only",
        action="store_true",
        help="Write plan and sbatch files without submitting",
    )
    parser.add_argument("--output", default=None, help="Write machine-readable dry-run output to this path")


def add_eval_source_args(parser: argparse.ArgumentParser, *, required: bool) -> None:
    group = parser.add_mutually_exclusive_group(required=required)
    group.add_argument("--from-train-batch", dest="from_train_batch", help="Path to a train stage batch root")
    group.add_argument("--from-run", dest="from_run", help="Path to a run directory with stage_outputs.json")
    group.add_argument("--checkpoint", help="Explicit checkpoint path for all selected eval runs")
    parser.add_argument("--input-name", default=None, help="Eval input to bind from the selected source")


def load_spec_from_args(args: argparse.Namespace) -> ExperimentSpec:
    return load_experiment_spec(
        Path(args.config),
        cli_overrides=tuple(args.set),
        project_root=None if args.project_root is None else Path(args.project_root),
    )


def resolve_eval_input_source(
    spec: ExperimentSpec,
    source: EvalInputSourceRequest,
) -> tuple[tuple[Any, ...], dict[str, tuple[Any, ...]], str]:
    return resolve_eval_inputs(
        spec,
        from_train_batch=source.value if source.kind == "from_train_batch" else None,
        from_run=source.value if source.kind == "from_run" else None,
        checkpoint=source.value if source.kind == "checkpoint" else None,
        input_name=source.input_name,
    )


def print_lines(lines: list[str]) -> None:
    for line in lines:
        print(line)


def execution_mode_from_args(args: argparse.Namespace, *, default: ExecutionMode = "submit") -> ExecutionMode:
    if getattr(args, "dry_run", False):
        return "preview"
    if getattr(args, "emit_only", False):
        return "emit"
    return default


def dry_run_mode_from_args(args: argparse.Namespace) -> str:
    value = getattr(args, "dry_run", False)
    if value is True:
        return "summary"
    if value in (False, None, ""):
        return ""
    return str(value)


def emit_machine_dry_run_if_requested(
    args: argparse.Namespace,
    spec: ExperimentSpec,
    plan,
    *,
    command: str,
) -> bool:
    mode = dry_run_mode_from_args(args)
    if mode not in {"json", "full"}:
        return False
    audit = build_dry_run_audit(spec, plan, command=command, full=mode == "full")
    payload = json.dumps(to_jsonable(audit), indent=2, sort_keys=True) + "\n"
    output = getattr(args, "output", None)
    if output:
        Path(output).write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    return True


def load_snapshot_yaml(root: Path) -> dict[str, Any]:
    path = root / "spec_snapshot.yaml"
    if not path.exists():
        raise FileNotFoundError(f"spec_snapshot.yaml not found under {root}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"spec_snapshot.yaml must contain a mapping: {path}")
    return payload


def build_train_batch_from_args(args: argparse.Namespace):
    spec = load_spec_from_args(args)
    batch = build_train_stage_batch(spec)
    return spec, batch


def build_eval_batch_from_args(args: argparse.Namespace):
    spec = load_spec_from_args(args)
    source = eval_source_from_args(args)
    if source is None:
        raise ValueError("eval requires one of --from-train-batch, --from-run, or --checkpoint")
    batch = build_eval_stage_batch(
        spec,
        from_train_batch=source.value if source.kind == "from_train_batch" else None,
        from_run=source.value if source.kind == "from_run" else None,
        checkpoint=source.value if source.kind == "checkpoint" else None,
        input_name=source.input_name,
    )
    return spec, batch


def build_train_eval_pipeline_from_args(args: argparse.Namespace):
    spec = load_spec_from_args(args)
    plan = build_train_eval_pipeline_plan(spec)
    return spec, plan


def print_stage_batch_plan(batch) -> None:
    print_lines(summarize_stage_batch(batch))


def print_train_eval_pipeline_plan(plan) -> None:
    print_lines(summarize_train_eval_pipeline_plan(plan))
