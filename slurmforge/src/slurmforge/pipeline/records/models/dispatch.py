from __future__ import annotations

from dataclasses import dataclass

from ..codecs.array_assignment import ensure_array_assignment
from .array_assignment import ArrayAssignment


@dataclass(frozen=True)
class DispatchInfo:
    sbatch_path: str = ""
    sbatch_path_rel: str | None = None
    record_path: str | None = None
    record_path_rel: str | None = None
    array_group: int | None = None
    array_task_index: int | None = None
    array_assignment: ArrayAssignment | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sbatch_path", str(self.sbatch_path or ""))
        object.__setattr__(self, "sbatch_path_rel", None if self.sbatch_path_rel in (None, "") else str(self.sbatch_path_rel))
        object.__setattr__(self, "record_path", None if self.record_path in (None, "") else str(self.record_path))
        object.__setattr__(self, "record_path_rel", None if self.record_path_rel in (None, "") else str(self.record_path_rel))
        object.__setattr__(self, "array_group", None if self.array_group is None else int(self.array_group))
        object.__setattr__(self, "array_task_index", None if self.array_task_index is None else int(self.array_task_index))
        object.__setattr__(
            self,
            "array_assignment",
            None if self.array_assignment is None else ensure_array_assignment(self.array_assignment),
        )
