from __future__ import annotations

from pathlib import Path

from ..control.workflow import PipelineAdvanceResult, advance_pipeline_once
from ..workflow_contract import PIPELINE_GATES
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH

PIPELINE_GATE_CHOICES = PIPELINE_GATES


def resume_train_eval_pipeline(
    pipeline_root: Path,
    *,
    gate: str | None = None,
    group_id: str | None = None,
    client: SlurmClientProtocol | None = None,
    missing_output_grace_seconds: int = 300,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> PipelineAdvanceResult:
    return advance_pipeline_once(
        pipeline_root,
        gate=gate,
        group_id=group_id,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
        max_dependency_length=max_dependency_length,
    )
