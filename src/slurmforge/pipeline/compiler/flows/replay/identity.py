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
    context = materialized.context
    assert isinstance(context, ReplayMaterializedState)
    candidate_identity = resolve_replay_batch_identity(
        spec,
        project_root=context.project_root,
        default_batch_name=context.default_batch_name,
        parsed_overrides=context.parsed_overrides,
    )
    if state.identity is None:
        return CompileState(
            identity=candidate_identity,
            notify_cfg=spec.notify,
            submit_dependencies=copy.deepcopy(spec.output.dependencies),
            batch_diagnostics=state.batch_diagnostics,
        )
    if candidate_identity != state.identity:
        raise InternalCompilerError("Replay batch must resolve to a single canonical batch identity")
    if spec.notify != state.notify_cfg:
        raise InternalCompilerError("Replay batch must resolve to a single canonical notify configuration")
    if spec.output.dependencies != state.submit_dependencies:
        raise InternalCompilerError("Replay batch must resolve to a single canonical submit dependency mapping")
    return state
