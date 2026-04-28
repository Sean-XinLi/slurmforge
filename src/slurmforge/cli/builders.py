from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ..errors import UsageError
from ..orchestration import (
    build_eval_stage_batch,
    build_train_eval_pipeline_plan,
    build_train_stage_batch,
    resolve_eval_inputs,
)
from ..spec import ExperimentSpec, load_experiment_spec
from .requests import EvalInputSourceRequest, eval_source_from_args


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


def build_train_batch_from_args(args: argparse.Namespace):
    spec = load_spec_from_args(args)
    batch = build_train_stage_batch(spec)
    return spec, batch


def build_eval_batch_from_args(args: argparse.Namespace):
    spec = load_spec_from_args(args)
    source = eval_source_from_args(args)
    if source is None:
        raise UsageError("eval requires one of --from-train-batch, --from-run, or --checkpoint")
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
