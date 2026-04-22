from __future__ import annotations

import copy

from .....errors import InternalCompilerError
from ....sources.replay import resolve_replay_batch_identity
from ...state import MaterializedSourceBundle, ReplayMaterializedState
from ...state import CompileState


def initialize_replay_compile_state(
    materialized: MaterializedSourceBundle,
    *,
    include_planning: bool,
) -> CompileState:
    return CompileState(batch_diagnostics=materialized.batch_diagnostics)


def accept_replay_spec(
    state: CompileState,
    *,
    spec,
    materialized: MaterializedSourceBundle,
    source_input,
) -> CompileState:
    from dataclasses import replace

    context = materialized.context
    assert isinstance(context, ReplayMaterializedState)
    candidate_identity = resolve_replay_batch_identity(
        spec,
        project_root=context.project_root,
        default_batch_name=context.default_batch_name,
        parsed_overrides=context.parsed_overrides,
    )

    # Batch-scoped identity / notify / output.dependencies / storage use
    # first-wins + raise-on-mismatch.  These are things replay assumes are
    # uniform across the reconstructed batch and the user has no CLI path
    # to diverge them per run.
    if state.identity is None:
        base = CompileState(
            identity=candidate_identity,
            notify_cfg=spec.notify,
            submit_dependencies=copy.deepcopy(spec.output.dependencies),
            batch_diagnostics=state.batch_diagnostics,
            storage_config=spec.storage,
        )
    else:
        if candidate_identity != state.identity:
            raise InternalCompilerError("Replay batch must resolve to a single canonical batch identity")
        if spec.notify != state.notify_cfg:
            raise InternalCompilerError("Replay batch must resolve to a single canonical notify configuration")
        if spec.output.dependencies != state.submit_dependencies:
            raise InternalCompilerError("Replay batch must resolve to a single canonical submit dependency mapping")
        base = state

    # Batch-scoped GPU budget and dispatch policy: accumulate candidates
    # and resolve at report-build time.  This lets the user use
    # ``--set resources.max_available_gpus=X`` to unify runs pulled from
    # different batches (the override is already baked into each spec's
    # raw cfg by the replay source loader).
    return replace(
        base,
        max_available_gpus_candidates=base.max_available_gpus_candidates + (int(spec.resources.max_available_gpus),),
        dispatch_policy_candidates=base.dispatch_policy_candidates + (str(spec.dispatch.group_overflow_policy),),
    )
