from __future__ import annotations

from pathlib import Path
import sys

from ..io import read_json
from ..slurm import SlurmClient
from ..status.reconcile import reconcile_stage_batch_with_slurm
from ..storage.plan_reader import load_execution_stage_batch_plan
from .delivery import deliver_notification
from .read_model import load_notification_summary_input, notification_plan_for_root


def _submitted_group_job_ids(batch_root: Path) -> dict[str, str]:
    path = Path(batch_root) / "submissions" / "ledger.json"
    if not path.exists():
        return {}
    payload = read_json(path)
    groups = dict(payload.get("groups") or {})
    return {
        group_id: str(record["scheduler_job_id"])
        for group_id, record in groups.items()
        if record.get("scheduler_job_id")
    }


def _reconcile_batch_submission(batch_root: Path, *, missing_output_grace_seconds: int) -> None:
    group_job_ids = _submitted_group_job_ids(batch_root)
    if not group_job_ids:
        return
    batch = load_execution_stage_batch_plan(batch_root)
    reconcile_stage_batch_with_slurm(
        batch,
        group_job_ids=group_job_ids,
        client=SlurmClient(),
        missing_output_grace_seconds=missing_output_grace_seconds,
    )


def run_finalizer(
    root: Path,
    *,
    event: str,
    missing_output_grace_seconds: int = 300,
) -> int:
    target = Path(root).resolve()
    if event == "batch_finished":
        _reconcile_batch_submission(
            target,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
    record = deliver_notification(
        target,
        event=event,
        notification_plan=notification_plan_for_root(target),
        summary_input=load_notification_summary_input(target, event=event),
    )
    if record is not None and record.state == "failed":
        return 1
    return 0


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Send a slurmforge terminal summary notification")
    parser.add_argument("--root", required=True)
    parser.add_argument("--event", required=True, choices=("batch_finished", "train_eval_pipeline_finished"))
    parser.add_argument("--missing-output-grace-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    raise SystemExit(
        run_finalizer(
            Path(args.root),
            event=args.event,
            missing_output_grace_seconds=args.missing_output_grace_seconds,
        )
    )


if __name__ == "__main__":
    main(sys.argv[1:])
