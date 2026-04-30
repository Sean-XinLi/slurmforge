from __future__ import annotations

from pathlib import Path

from ..control.workflow import AdvanceHint, PipelineAdvanceResult, advance_pipeline_once
from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH


def resume_train_eval_pipeline(
    pipeline_root: Path,
    *,
    event: str | None = None,
    stage_instance_id: str | None = None,
    client: SlurmClientProtocol | None = None,
    missing_output_grace_seconds: int = 300,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> PipelineAdvanceResult:
    hint = None
    if event is not None:
        hint = AdvanceHint(event=event, stage_instance_id=stage_instance_id or "")
    return advance_pipeline_once(
        pipeline_root,
        hint=hint,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
        max_dependency_length=max_dependency_length,
    )
