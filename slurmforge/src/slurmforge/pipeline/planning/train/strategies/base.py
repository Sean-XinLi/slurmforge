from __future__ import annotations

from abc import ABC, abstractmethod

from ...contracts import StageExecutionPlan
from ..context import TrainContext


class TrainModeStrategy(ABC):
    mode = "base"

    @abstractmethod
    def build(self, ctx: TrainContext) -> StageExecutionPlan:
        raise NotImplementedError
