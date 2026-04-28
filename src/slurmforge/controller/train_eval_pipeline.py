from __future__ import annotations

import sys
from pathlib import Path

from ..io import write_exception_diagnostic
from ..slurm import SlurmClient, SlurmClientProtocol
from ..spec import load_experiment_spec_from_snapshot
from ..storage.controller import write_controller_status
from ..storage.plan_reader import load_stage_batch_plan, load_train_eval_pipeline_plan
from .materialization import ensure_stage_materialized, project_root_from_pipeline
from .state import load_controller_state, record_controller_event, save_controller_state
from .stage_runtime import (
    batch_terminal,
    mark_stage_completed,
    submit_stage_once,
    wait_terminal,
)
from .terminal import complete_pipeline


def run_controller(
    pipeline_root: Path,
    *,
    client: SlurmClientProtocol | None = None,
    poll_seconds: int = 30,
    missing_output_grace_seconds: int = 300,
) -> int:
    slurm = client or SlurmClient()
    plan = load_train_eval_pipeline_plan(pipeline_root)
    state = load_controller_state(pipeline_root, plan)
    write_controller_status(pipeline_root, "running")
    try:
        spec = load_experiment_spec_from_snapshot(
            pipeline_root,
            project_root=project_root_from_pipeline(pipeline_root),
        )
        for stage_name in plan.stage_order:
            state["current_stage"] = stage_name
            state["state"] = "checking_stage"
            save_controller_state(pipeline_root, state)
            if stage_name in set(state.get("completed_stages") or []):
                continue
            batch = plan.stage_batches[stage_name]
            stage_spec = spec.enabled_stages[stage_name]
            if stage_spec.depends_on:
                batch = ensure_stage_materialized(
                    pipeline_root, plan, spec, state, stage_name
                )
            else:
                batch = load_stage_batch_plan(Path(batch.submission_root))
            if batch_terminal(Path(batch.submission_root)):
                mark_stage_completed(pipeline_root, state, stage_name)
                continue
            submit_stage_once(pipeline_root, state, batch, client=slurm)
            wait_terminal(
                batch,
                client=slurm,
                poll_seconds=poll_seconds,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
            mark_stage_completed(pipeline_root, state, stage_name)
        final_state = complete_pipeline(
            pipeline_root, state, notification_plan=plan.notification_plan
        )
        return 0 if final_state == "success" else 1
    except Exception as exc:
        write_exception_diagnostic(pipeline_root / "controller_traceback.log", exc)
        state["state"] = "failed"
        save_controller_state(pipeline_root, state)
        write_controller_status(pipeline_root, "failed", reason=str(exc))
        record_controller_event(pipeline_root, "controller_failed", reason=str(exc))
        return 1


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Run a slurmforge train/eval pipeline controller"
    )
    parser.add_argument("--train-eval-pipeline-root", required=True)
    args = parser.parse_args(argv)
    raise SystemExit(run_controller(Path(args.train_eval_pipeline_root).resolve()))


if __name__ == "__main__":
    main(sys.argv[1:])
