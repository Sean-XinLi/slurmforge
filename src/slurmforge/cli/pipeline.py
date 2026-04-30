from __future__ import annotations

import argparse
from pathlib import Path

from ..orchestration.pipeline import resume_train_eval_pipeline


def handle_pipeline_resume(args: argparse.Namespace) -> None:
    result = resume_train_eval_pipeline(
        Path(args.root).resolve(),
        event=args.event,
        stage_instance_id=args.stage_instance_id,
    )
    print(f"[OK] pipeline state={result.state}")
    if result.submitted_stage_job_ids:
        for stage_name, job_ids in sorted(result.submitted_stage_job_ids.items()):
            print(f"[OK] stage={stage_name} jobs={','.join(job_ids.values())}")
    if result.submitted_control_job_ids:
        for key, job_ids in sorted(result.submitted_control_job_ids.items()):
            print(f"[OK] control={key} jobs={','.join(job_ids)}")
    print(f"[OK] pipeline_root={result.pipeline_root}")


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "pipeline", help="Operate on a streaming train/eval pipeline"
    )
    pipeline_subparsers = parser.add_subparsers(dest="pipeline_command")
    pipeline_subparsers.required = True

    resume_parser = pipeline_subparsers.add_parser(
        "resume", help="Advance a streaming train/eval pipeline once"
    )
    resume_parser.add_argument("root", help="Train/eval pipeline root")
    resume_parser.add_argument(
        "--event",
        choices=("stage-instance-finished",),
        default=None,
    )
    resume_parser.add_argument("--stage-instance-id", default=None)
    resume_parser.set_defaults(handler=handle_pipeline_resume)
