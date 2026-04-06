from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ArrayAssignment:
    group_index: int | None = None
    group_signature: str = ""
    grouping_fields: dict[str, Any] = field(default_factory=dict)
    group_reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "group_index", None if self.group_index is None else int(self.group_index))
        object.__setattr__(self, "group_signature", str(self.group_signature or ""))
        object.__setattr__(self, "grouping_fields", copy.deepcopy(dict(self.grouping_fields or {})))
        object.__setattr__(self, "group_reason", str(self.group_reason or ""))
