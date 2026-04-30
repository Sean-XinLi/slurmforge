from __future__ import annotations

from pathlib import Path

from ..contracts import (
    InputBinding,
    InputSource,
    RunDefinition,
    binding_is_ready_for_injection,
)
from ..plans.train_eval import TrainEvalPipelinePlan
from ..spec import ExperimentSpec, StageInputSpec
from ..status.reader import read_stage_status
from ..outputs.records import load_stage_outputs
from ..storage.plan_reader import plan_for_run_dir, run_definitions_from_stage_batch
from .binding_builders import (
    inject_payload,
    path_binding_for_spec,
    unresolved_binding,
)
from .models import ResolvedStageInputs
from .output_refs import (
    output_ref,
    resolved_output,
    upstream_resolution,
)


def _producer_ref(
    input_spec: StageInputSpec, depends_on: tuple[str, ...]
) -> tuple[str, str] | None:
    if input_spec.source.kind == "upstream_output":
        return input_spec.source.stage, input_spec.source.output
    return None


def _pipeline_upstream_binding(
    plan: TrainEvalPipelinePlan,
    input_spec: StageInputSpec,
    run: RunDefinition,
    *,
    producer_stage: str,
    output_name: str,
) -> InputBinding:
    producer_batch = plan.stage_batches.get(producer_stage)
    if producer_batch is None:
        return unresolved_binding(
            input_spec,
            source=InputSource(
                kind="upstream_output", stage=producer_stage, output=output_name
            ),
            reason=f"producer stage `{producer_stage}` is not in the pipeline plan",
        )
    producer_root = Path(producer_batch.submission_root)
    producer_run_dir = producer_root / f"runs/{run.run_id}"
    producer_plan = plan_for_run_dir(producer_run_dir)
    if producer_plan is None:
        return unresolved_binding(
            input_spec,
            source=InputSource(
                kind="upstream_output", stage=producer_stage, output=output_name
            ),
            reason=f"producer stage plan was not found for run `{run.run_id}`",
        )
    status = read_stage_status(producer_run_dir)
    if status is None or status.state != "success":
        state = "missing" if status is None else status.state
        return unresolved_binding(
            input_spec,
            source=InputSource(
                kind="upstream_output", stage=producer_stage, output=output_name
            ),
            reason=f"producer stage `{producer_stage}` is not successful for run `{run.run_id}`: {state}",
        )
    outputs = load_stage_outputs(producer_run_dir)
    output = None if outputs is None else output_ref(outputs, output_name)
    if output is None:
        return unresolved_binding(
            input_spec,
            source=InputSource(
                kind="upstream_output", stage=producer_stage, output=output_name
            ),
            reason=f"producer output `{output_name}` was not found for run `{run.run_id}`",
        )
    return InputBinding(
        input_name=input_spec.name,
        source=InputSource(
            kind="upstream_output", stage=producer_plan.stage_name, output=output_name
        ),
        expects=input_spec.expects,
        resolved=resolved_output(output),
        inject=inject_payload(input_spec),
        resolution=upstream_resolution(
            producer_root=producer_root,
            run_dir=producer_run_dir,
            stage_instance_id=producer_plan.stage_instance_id,
            run_id=producer_plan.run_id,
            stage_name=producer_plan.stage_name,
            output_name=output_name,
            output=output,
        ),
    )


def _resolve_input_for_pipeline(
    spec: ExperimentSpec,
    plan: TrainEvalPipelinePlan,
    stage_name: str,
    run: RunDefinition,
    input_spec: StageInputSpec,
) -> InputBinding:
    if input_spec.source.kind == "external_path":
        return path_binding_for_spec(spec, input_spec)
    producer = _producer_ref(input_spec, spec.enabled_stages[stage_name].depends_on)
    if producer is not None:
        producer_stage, output_name = producer
        return _pipeline_upstream_binding(
            plan,
            input_spec,
            run,
            producer_stage=producer_stage,
            output_name=output_name,
        )
    return unresolved_binding(
        input_spec,
        reason="input source is not resolvable by the train/eval pipeline control plane",
    )


def resolve_stage_inputs_for_train_eval_pipeline(
    spec: ExperimentSpec,
    plan: TrainEvalPipelinePlan,
    *,
    stage_name: str,
    runs: tuple[RunDefinition, ...] | None = None,
) -> ResolvedStageInputs:
    stage = spec.enabled_stages[stage_name]
    run_defs = (
        run_definitions_from_stage_batch(plan.stage_batches[stage_name])
        if runs is None
        else runs
    )
    selected_runs: list[RunDefinition] = []
    bindings_by_run: dict[str, tuple[InputBinding, ...]] = {}
    blocked: list[str] = []
    blocked_reasons: dict[str, str] = {}
    for run in run_defs:
        bindings = tuple(
            _resolve_input_for_pipeline(
                spec, plan, stage_name, run, stage.inputs[input_name]
            )
            for input_name in sorted(stage.inputs)
        )
        failures = [
            f"{binding.input_name}: {binding.resolution.get('reason') or 'unresolved'}"
            for binding in bindings
            if binding.inject.get("required")
            and not binding_is_ready_for_injection(binding)
        ]
        if failures:
            blocked.append(run.run_id)
            blocked_reasons[run.run_id] = "; ".join(failures)
            continue
        selected_runs.append(run)
        bindings_by_run[run.run_id] = bindings
    return ResolvedStageInputs(
        stage_name=stage_name,
        selected_runs=tuple(selected_runs),
        input_bindings_by_run=bindings_by_run,
        blocked_run_ids=tuple(sorted(blocked)),
        blocked_reasons=blocked_reasons,
    )
