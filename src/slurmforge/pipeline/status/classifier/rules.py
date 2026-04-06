from __future__ import annotations

from pathlib import Path

from ..models import AttemptResult
from .patterns import NODE_FAILURE_PATTERNS, OOM_PATTERNS, PREEMPTED_PATTERNS
from .reader import read_combined_log_text


def match_patterns(text: str, patterns: tuple[str, ...]) -> str | None:
    lower = text.lower()
    for pattern in patterns:
        if pattern in lower:
            return pattern
    return None


def classify_log_text(text: str, *, failed_stage: str) -> tuple[str, str, str] | None:
    if not text:
        return None
    oom_match = match_patterns(text, OOM_PATTERNS)
    if oom_match is not None:
        return "oom", failed_stage, f"matched OOM signal/pattern `{oom_match}`"
    preempt_match = match_patterns(text, PREEMPTED_PATTERNS)
    if preempt_match is not None:
        return "preempted", failed_stage, f"matched preemption pattern `{preempt_match}`"
    node_match = match_patterns(text, NODE_FAILURE_PATTERNS)
    if node_match is not None:
        return "node_failure", failed_stage, f"matched node failure pattern `{node_match}`"
    return None


def classify_failure(
    *,
    shell_exit_code: int,
    attempt: AttemptResult | None,
    result_dir: Path,
) -> tuple[str | None, str | None, str]:
    train_exit = attempt.train_exit_code if attempt else None
    eval_exit = attempt.eval_exit_code if attempt else None
    if train_exit not in {None, 0}:
        failed_stage = "train"
    elif eval_exit not in {None, 0}:
        failed_stage = "eval"
    else:
        failed_stage = "executor"

    combined_text = read_combined_log_text(
        attempt,
        result_dir,
        slurm_job_id=attempt.slurm_job_id if attempt is not None else "",
    )
    log_classification = classify_log_text(combined_text, failed_stage=failed_stage)
    if log_classification is not None:
        return log_classification
    if failed_stage in {"train", "eval"} and (train_exit == 137 or eval_exit == 137):
        return "oom", failed_stage, "matched OOM signal/pattern `exit_code_137`"

    if failed_stage == "eval":
        return "eval_failed", failed_stage, f"eval_exit_code={eval_exit}"
    if failed_stage == "train":
        return "script_error", failed_stage, f"train_exit_code={train_exit}"
    if shell_exit_code != 0 or attempt is None:
        return "executor_error", failed_stage, f"shell_exit_code={shell_exit_code}"
    return "script_error", failed_stage, "non-zero execution status without matching failure pattern"


def classify_logs_only(result_dir: Path, *, slurm_job_id: str = "") -> tuple[str, str, str] | None:
    combined_text = read_combined_log_text(None, result_dir, slurm_job_id=slurm_job_id)
    return classify_log_text(combined_text, failed_stage="executor")
