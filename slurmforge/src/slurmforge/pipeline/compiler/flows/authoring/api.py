from __future__ import annotations

from ....config.api import ExperimentSpec
from ....sources.models import SourceRunInput
from ...base import CompilerFlowBase
from ...reports.models import BatchCompileReport
from ...requests import AuthoringSourceRequest, SourceRequest
from ...state import CollectedSourceBundle, CompileState, MaterializedSourceBundle
from .collect import collect_authoring_source_bundle
from .context import materialize_authoring_bundle
from .identity import initialize_authoring_compile_state
from .spec_builder import accept_authoring_spec, build_authoring_spec


class AuthoringCompilerFlow(CompilerFlowBase):
    request_types = (AuthoringSourceRequest,)

    def collect(self, source: SourceRequest) -> CollectedSourceBundle:
        assert isinstance(source, AuthoringSourceRequest)
        return collect_authoring_source_bundle(source)

    def materialize(self, bundle: CollectedSourceBundle) -> MaterializedSourceBundle:
        return materialize_authoring_bundle(bundle)

    def initialize_compile_state(
        self,
        materialized: MaterializedSourceBundle,
        *,
        include_planning: bool,
    ) -> CompileState | BatchCompileReport:
        return initialize_authoring_compile_state(materialized, include_planning=include_planning)

    def build_spec(
        self,
        materialized: MaterializedSourceBundle,
        source_input: SourceRunInput,
    ) -> ExperimentSpec:
        return build_authoring_spec(materialized, source_input)

    def accept_spec(
        self,
        state: CompileState,
        *,
        spec: ExperimentSpec,
        materialized: MaterializedSourceBundle,
        source_input: SourceRunInput,
    ) -> CompileState:
        return accept_authoring_spec(
            state,
            spec=spec,
            materialized=materialized,
            source_input=source_input,
        )


AUTHORING_FLOW = AuthoringCompilerFlow()
