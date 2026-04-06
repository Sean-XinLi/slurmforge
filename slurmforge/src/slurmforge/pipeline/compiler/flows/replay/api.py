from __future__ import annotations

from ....config.api import ExperimentSpec
from ....sources.models import SourceRunInput
from ...base import CompilerFlowBase
from ...reports.models import BatchCompileReport
from ...requests import ReplaySourceRequest, RetrySourceRequest, SourceRequest
from ...state import CollectedSourceBundle, CompileState, MaterializedSourceBundle
from .collect import collect_replay_source_bundle, collect_retry_source_bundle
from .context import materialize_replay_bundle
from .identity import accept_replay_spec, initialize_replay_compile_state
from .spec_builder import build_replay_spec


class ReplayCompilerFlow(CompilerFlowBase):
    request_types = (ReplaySourceRequest, RetrySourceRequest)

    def collect(self, source: SourceRequest) -> CollectedSourceBundle:
        if isinstance(source, ReplaySourceRequest):
            return collect_replay_source_bundle(source)
        assert isinstance(source, RetrySourceRequest)
        return collect_retry_source_bundle(source)

    def materialize(self, bundle: CollectedSourceBundle) -> MaterializedSourceBundle:
        return materialize_replay_bundle(bundle)

    def initialize_compile_state(
        self,
        materialized: MaterializedSourceBundle,
        *,
        include_planning: bool,
    ) -> CompileState | BatchCompileReport:
        return initialize_replay_compile_state(materialized, include_planning=include_planning)

    def build_spec(
        self,
        materialized: MaterializedSourceBundle,
        source_input: SourceRunInput,
    ) -> ExperimentSpec:
        return build_replay_spec(materialized, source_input)

    def accept_spec(
        self,
        state: CompileState,
        *,
        spec: ExperimentSpec,
        materialized: MaterializedSourceBundle,
        source_input: SourceRunInput,
    ) -> CompileState:
        return accept_replay_spec(
            state,
            spec=spec,
            materialized=materialized,
            source_input=source_input,
        )


REPLAY_FLOW = ReplayCompilerFlow()
