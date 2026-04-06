from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class OutputConfigSpec:
    base_output_dir: str = "./runs"
    batch_name: str | None = None
    dependencies: dict[str, list[str]] = field(default_factory=dict)
