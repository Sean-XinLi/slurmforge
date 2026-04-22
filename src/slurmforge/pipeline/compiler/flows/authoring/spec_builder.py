from __future__ import annotations

from ....config.assembly.batch_contract import validate_batch_contract
from ....config.api import ExperimentSpec
from ....config.assembly.spec_builder import materialize_authoring_experiment_spec
from ....sources.models import SourceRunInput
from ...state import AuthoringMaterializedState, CompileState, MaterializedSourceBundle


def build_authoring_spec(
    materialized: MaterializedSourceBundle,
    source_input: SourceRunInput,
) -> ExperimentSpec:
    context = materialized.context
    assert isinstance(context, AuthoringMaterializedState)
    spec = materialize_authoring_experiment_spec(
        source_input.run_cfg,
        config_path=context.config_path,
        project_root=context.project_root,
        batch_shared=context.shared,
        model_catalog_resolver=context.model_catalog_resolver,
    )
    validate_batch_contract(spec=spec, shared=context.shared, config_path=context.config_path)
    return spec


def accept_authoring_spec(
    state: CompileState,
    *,
    spec: ExperimentSpec,
    materialized: MaterializedSourceBundle,
    source_input: SourceRunInput,
) -> CompileState:
    return _append_batch_scope_candidates(state, spec=spec)


def _append_batch_scope_candidates(state: CompileState, *, spec: ExperimentSpec) -> CompileState:
    """Accumulate this spec's batch-scoped values onto the running state.

    The batch uses these candidates to resolve a single winning value
    (via ``resolve_batch_scope_unique``) at report-build time.  Authoring
    and replay flows share this mechanism so they behave identically.
    """
    from dataclasses import replace

    return replace(
        state,
        max_available_gpus_candidates=state.max_available_gpus_candidates + (int(spec.resources.max_available_gpus),),
        dispatch_policy_candidates=state.dispatch_policy_candidates + (str(spec.dispatch.group_overflow_policy),),
    )
