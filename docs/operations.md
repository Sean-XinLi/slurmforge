# Operations

## Execution Modes

- `--dry-run` previews the plan only. It writes no files and submits nothing.
- `--dry-run=json` writes a machine-readable plan audit to stdout or `--output`.
- `--dry-run=full` adds contract verification, runtime probes, and `resource_estimate`.
- `--emit-only` writes plan, sbatch, manifests, and a planned submission ledger, but submits nothing.
- No execution-mode flag writes files and submits.

`sforge plan train|eval|run` defaults to emit mode; `--dry-run` is preview-only.

## Status

`sforge status --from <root>` reads persisted status projections from a stage batch or train/eval pipeline root.

`sforge status --from <root> --reconcile` also queries Slurm through submission ledgers, appends scheduler observations, and refreshes aggregate `run_status.json` and `train_eval_pipeline_status.json` before printing.

Stage status is monotonic. A late submit marker cannot overwrite terminal `success`, `failed`, `cancelled`, or `blocked`.

## Resubmit

`sforge resubmit` is replan-based and lineage-aware. It selects runs from an existing root, applies `--set` overrides to the saved spec snapshot, recompiles only the requested stage into a new stage batch root, and resolves inputs from the new spec contract plus saved lineage.

Every materialized resubmit writes `source_plan.json` and `source_lineage.json` before submit-file generation. Derived batch roots are created under `<source_root>/derived_batches/`, so repeated resubmits do not mutate earlier batches.

## State Files

A stage batch root contains:

```text
<batch_root>/
  manifest.json
  lineage_index.json
  materialization_status.json
  run_status.json
  spec_snapshot.yaml
  batch_plan.json
  selected_batch_plan.json         # optional execution subset
  blocked_runs.json                # optional selected-run block list
  submit/
    submit_manifest.json
    submit.sh
    generations/
      gen_<digest>/
        group_001.sbatch
        notify_batch_finished.sbatch
        submit.sh
    notifications/
      gen_<digest>/
        barrier_batch_finished_001.sbatch
    logs/
      gen_<digest>/
  submissions/
    ledger.json
    events.jsonl
  notifications/
    records/
      batch_finished.email.json
    events.jsonl
  scheduler_observations.jsonl
  groups/
    groups.json
    selected_groups.json           # optional execution subset
    gpu_budget_plan.json
    selected_gpu_budget_plan.json  # optional execution subset
  runs/
    <run_id>/
      root_ref.json
      stage_plan.json
      input_bindings.json
      input_verification.json
      stage_outputs.json
      status.json
      status_events.jsonl
      attempts/
        0001/
          attempt.json
          runtime_probe.json
          artifacts/
            artifact_manifest.json
            files/
          outputs/
            stage_outputs.json
```

A train/eval pipeline root contains:

```text
<train_eval_pipeline_root>/
  manifest.json
  lineage_index.json
  spec_snapshot.yaml
  run_status.json
  train_eval_pipeline_plan.json
  train_eval_pipeline_status.json
  control/
    control_plan.json
    workflow_state.json
    workflow_status.json
    gate_ledger.json
    events.jsonl
    gates/
      train_initial_group_001_instance_gate.sbatch
      dispatch_catchup_train_initial_gate.sbatch
      eval_<dispatch_id>_group_001_instance_gate.sbatch
      dispatch_catchup_eval_<dispatch_id>_gate.sbatch
      *_barrier_001.sbatch          # optional dependency fan-in barriers
    logs/
  execution/
    stage_catalog.json
    runtime_batches.json
  notifications/
    records/
      train_eval_pipeline_finished.email.json
    events.jsonl
  stage_batches/
    train/
    eval/
```

## Scheduler Reconcile

Slurm reconcile observes both `sacct` and `squeue`. `squeue` covers active jobs that may not have reached accounting yet; `sacct` covers terminal history.

A completed Slurm job without `stage_outputs.json` remains in a waiting reconcile state briefly before it is classified as `missing_attempt_result`.

Scheduler reconcile can create a scheduler-sourced attempt record when Slurm reports a running or terminal task that did not leave an executor attempt. This preserves the task-to-attempt audit trail even when Python never started.
