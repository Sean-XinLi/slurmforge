from __future__ import annotations

from ..orchestration.pipeline_build import summarize_train_eval_pipeline_plan
from ..orchestration.results import (
    StageBatchExecutionResult,
    SourcedStageBatchExecutionResult,
    TrainEvalPipelineExecutionResult,
)
from ..orchestration.stage_build import (
    summarize_stage_batch,
)


def print_lines(lines: list[str]) -> None:
    for line in lines:
        print(line)


def print_stage_batch_execution_result(
    result: StageBatchExecutionResult, *, noun: str = "stage batch"
) -> None:
    if result.mode == "preview":
        return
    if result.submitted:
        print(f"[OK] submitted {noun}: {result.root}")
        print(f"[OK] scheduler_job_ids={','.join(result.scheduler_job_ids.values())}")
        return
    print(f"[OK] emitted {noun}: {result.root}")


def print_sourced_stage_batch_execution_result(
    result: SourcedStageBatchExecutionResult, *, noun: str
) -> None:
    print_stage_batch_execution_result(
        StageBatchExecutionResult(
            root=result.root,
            mode=result.mode,
            submitted=result.submitted,
            scheduler_job_ids=result.scheduler_job_ids,
            notification_job_id=result.notification_job_id,
        ),
        noun=noun,
    )


def print_train_eval_pipeline_execution_result(
    result: TrainEvalPipelineExecutionResult,
) -> None:
    if result.mode == "preview":
        return
    if result.submitted:
        print(
            f"[OK] submitted train/eval pipeline controller: {result.controller_job_id}"
        )
        print(f"[OK] pipeline_root={result.root}")
        return
    print(f"[OK] emitted train/eval pipeline: {result.root}")


def print_stage_batch_plan(batch) -> None:
    print_lines(summarize_stage_batch(batch))


def print_train_eval_pipeline_plan(plan) -> None:
    print_lines(summarize_train_eval_pipeline_plan(plan))
