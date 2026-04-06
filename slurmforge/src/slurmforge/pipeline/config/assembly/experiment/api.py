from __future__ import annotations

from pathlib import Path
from typing import Any

from ...models import BatchSharedSpec
from .assembler import NormalizedExperimentContract, assemble_experiment_contract
from .hints import build_planning_hints
from .inputs import gather_experiment_section_inputs
from .sections import normalize_experiment_sections


def normalize_experiment_contract(
    cfg: dict[str, Any],
    *,
    config_path: Path | str,
    batch_shared: BatchSharedSpec | None = None,
) -> NormalizedExperimentContract:
    inputs = gather_experiment_section_inputs(
        cfg,
        config_path=config_path,
        batch_shared=batch_shared,
    )
    sections = normalize_experiment_sections(inputs, batch_shared=batch_shared)
    hints = build_planning_hints(inputs)
    return assemble_experiment_contract(inputs, sections, hints=hints)
