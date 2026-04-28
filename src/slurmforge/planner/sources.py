from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Iterable

from ..overrides import deep_set, parse_override
from ..contracts import RunDefinition
from ..plans.sources import (
    PriorBatchLineage,
    SelectedStageRun,
    SourcedStageBatchPlan,
    StageBatchSource,
)
from ..root_model import iter_stage_run_dirs
from ..spec import load_spec_snapshot, parse_experiment_spec, validate_experiment_spec
from ..status import read_stage_status, state_matches
from ..storage.loader import plan_for_run_dir
from ..resolver.prior_source import resolve_stage_inputs_from_prior_source
from .stage_batch import compile_stage_batch


def select_stage_runs(
    source_root: Path,
    *,
    stage_name: str,
    query: str,
    run_ids: Iterable[str] = (),
) -> tuple[SelectedStageRun, ...]:
    selected_run_ids = set(run_ids)
    selected: list[SelectedStageRun] = []
    for run_dir in iter_stage_run_dirs(source_root):
        status = read_stage_status(run_dir)
        if status is None or status.stage_name != stage_name:
            continue
        if selected_run_ids and status.run_id not in selected_run_ids:
            continue
        if not state_matches(status, query):
            continue
        plan = plan_for_run_dir(run_dir)
        if plan is None:
            continue
        selected.append(
            SelectedStageRun(
                run_dir=run_dir,
                stage_instance_id=plan.stage_instance_id,
                run=RunDefinition(
                    run_id=plan.run_id,
                    run_index=plan.run_index,
                    run_overrides=dict(plan.run_overrides),
                    spec_snapshot_digest=plan.spec_snapshot_digest,
                ),
            )
        )
    return tuple(selected)


def _project_root_for_prior_source(
    source_root: Path, selected: tuple[SelectedStageRun, ...]
) -> Path:
    if not selected:
        return source_root
    first_plan = plan_for_run_dir(selected[0].run_dir)
    if first_plan is None:
        return source_root
    return Path(first_plan.lineage.get("project_root") or source_root).resolve()


def compile_stage_batch_from_prior_source(
    *,
    source_root: Path,
    stage_name: str,
    query: str = "state=failed",
    run_ids: Iterable[str] = (),
    overrides: Iterable[str] = (),
) -> SourcedStageBatchPlan | None:
    root = Path(source_root).resolve()
    selected = select_stage_runs(
        root, stage_name=stage_name, query=query, run_ids=run_ids
    )
    if not selected:
        return None
    raw = load_spec_snapshot(root)
    override_list = list(overrides)
    for override in override_list:
        key, value = parse_override(override)
        deep_set(raw, key, value)
    spec = parse_experiment_spec(
        raw,
        config_path=(root / "spec_snapshot.yaml").resolve(),
        project_root=_project_root_for_prior_source(root, selected),
    )
    validate_experiment_spec(spec)
    runs = tuple(item.run for item in selected)
    bindings_by_run = {
        run.run_id: resolve_stage_inputs_from_prior_source(
            spec=spec,
            source_root=root,
            stage_name=stage_name,
            run=run,
        )
        for run in runs
    }
    source = StageBatchSource(
        kind="prior_batch",
        source_root=str(root),
        stage=stage_name,
        query=query,
        run_ids=tuple(run_ids),
        overrides=tuple(override_list),
    )
    source_ref = f"prior_batch:{root}"
    batch = compile_stage_batch(
        spec,
        stage_name=stage_name,
        runs=runs,
        source_ref=source_ref,
        input_bindings_by_run=bindings_by_run,
    )
    batch = replace(
        batch,
        submission_root=str((root / "derived_batches" / batch.batch_id).resolve()),
    )
    lineage = PriorBatchLineage(
        source_root=str(root),
        stage=stage_name,
        query=query,
        selected_run_ids=tuple(item.run.run_id for item in selected),
        selected_stage_instance_ids=tuple(item.stage_instance_id for item in selected),
        overrides=tuple(override_list),
    )
    return SourcedStageBatchPlan(
        spec_snapshot=dict(spec.raw),
        batch=batch,
        source=source,
        lineage=lineage,
        selected_runs=selected,
    )
