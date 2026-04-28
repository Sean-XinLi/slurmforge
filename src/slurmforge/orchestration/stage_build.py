from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..errors import UsageError
from ..planner import (
    compile_stage_batch_from_prior_source,
    compile_stage_batch_for_kind,
    summarize_stage_batch as _summarize_stage_batch,
)
from ..plans import SourcedStageBatchPlan
from ..resolver import explicit_input_bindings, upstream_bindings_from_run, upstream_bindings_from_stage_batch
from ..spec import ExperimentSpec, stage_source_input_name


def resolve_eval_inputs(
    spec: ExperimentSpec,
    *,
    from_train_batch: str | None,
    from_run: str | None,
    checkpoint: str | None,
    input_name: str | None = None,
) -> tuple[tuple[Any, ...], dict[str, tuple[Any, ...]], str]:
    selected_input = input_name or stage_source_input_name(spec, stage_name="eval")
    if checkpoint:
        runs, bindings = explicit_input_bindings(
            spec,
            selected_input,
            Path(checkpoint),
            source_role="checkpoint",
        )
        first_binding = bindings[runs[0].run_id][0]
        return runs, bindings, f"checkpoint:{first_binding.resolved.path}"
    if from_train_batch:
        runs, bindings = upstream_bindings_from_stage_batch(
            spec,
            Path(from_train_batch).resolve(),
            input_name=selected_input,
        )
        return runs, bindings, f"train_batch:{Path(from_train_batch).resolve()}"
    if from_run:
        runs, bindings = upstream_bindings_from_run(spec, Path(from_run).resolve(), input_name=selected_input)
        return runs, bindings, f"run:{Path(from_run).resolve()}"
    raise UsageError("eval requires one of --from-train-batch, --from-run, or --checkpoint")


def build_train_stage_batch(spec: ExperimentSpec):
    return compile_stage_batch_for_kind(spec, kind="train")


def build_eval_stage_batch(
    spec: ExperimentSpec,
    *,
    from_train_batch: str | None = None,
    from_run: str | None = None,
    checkpoint: str | None = None,
    input_name: str | None = None,
    allow_unresolved: bool = False,
):
    if allow_unresolved and not (from_train_batch or from_run or checkpoint):
        return compile_stage_batch_for_kind(spec, kind="eval")
    runs, bindings, source_ref = resolve_eval_inputs(
        spec,
        from_train_batch=from_train_batch,
        from_run=from_run,
        checkpoint=checkpoint,
        input_name=input_name,
    )
    return compile_stage_batch_for_kind(
        spec,
        kind="eval",
        runs=runs,
        input_bindings_by_run=bindings,
        source_ref=source_ref,
    )


def summarize_stage_batch(batch) -> list[str]:
    return _summarize_stage_batch(batch)


def build_prior_source_stage_batch(
    *,
    source_root: Path,
    stage_name: str,
    query: str = "state=failed",
    run_ids: Iterable[str] = (),
    overrides: Iterable[str] = (),
) -> SourcedStageBatchPlan | None:
    return compile_stage_batch_from_prior_source(
        source_root=source_root,
        stage_name=stage_name,
        query=query,
        run_ids=run_ids,
        overrides=overrides,
    )
