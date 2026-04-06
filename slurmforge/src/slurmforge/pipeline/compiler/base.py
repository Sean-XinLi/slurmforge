from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from ..config.api import ExperimentSpec
from ..sources.models import SourceRunInput
from .reports.models import BatchCompileReport
from .requests import SourceRequest
from .state import CollectedSourceBundle, CompileState, MaterializedSourceBundle


class CompilerFlowBase(ABC):
    """Abstract base class for compiler flow strategies.

    A flow encapsulates the full compile lifecycle for a family of
    ``SourceRequest`` types: collecting sources, materializing context,
    building ``ExperimentSpec`` instances, and accepting them into the
    running ``CompileState``.

    Concrete subclasses are registered in ``compiler/api.py::STRATEGIES``
    and selected at runtime by ``resolve_strategy`` based on ``request_types``.

    Class attributes
    ----------------
    request_types : tuple[type[SourceRequest], ...]
        The ``SourceRequest`` subclasses this flow handles.  Must be defined
        as a class-level attribute on every concrete subclass.
    """

    request_types: ClassVar[tuple[type[SourceRequest], ...]]

    @abstractmethod
    def collect(self, source: SourceRequest) -> CollectedSourceBundle:
        """Collect raw source inputs for the given request."""
        ...

    @abstractmethod
    def materialize(self, bundle: CollectedSourceBundle) -> MaterializedSourceBundle:
        """Resolve project context and prepare the bundle for compilation."""
        ...

    @abstractmethod
    def initialize_compile_state(
        self,
        materialized: MaterializedSourceBundle,
        *,
        include_planning: bool,
    ) -> CompileState | BatchCompileReport:
        """
        Build the initial ``CompileState`` for this batch.

        Returns a ``BatchCompileReport`` directly when the batch cannot
        proceed (e.g. identity resolution failure), bypassing per-run
        compilation.
        """
        ...

    @abstractmethod
    def build_spec(
        self,
        materialized: MaterializedSourceBundle,
        source_input: SourceRunInput,
    ) -> ExperimentSpec:
        """Assemble a fully-resolved ``ExperimentSpec`` for one run."""
        ...

    @abstractmethod
    def accept_spec(
        self,
        state: CompileState,
        *,
        spec: ExperimentSpec,
        materialized: MaterializedSourceBundle,
        source_input: SourceRunInput,
    ) -> CompileState:
        """
        Incorporate an accepted spec into the running compile state.

        Returns the updated ``CompileState`` after recording any
        batch-level information derived from ``spec``.
        """
        ...
