# Status And Storage Records

## Storage Layout

Stage batch root:

```text
<batch_root>/
  manifest.json
  lineage_index.json
  materialization_status.json
  run_status.json
  spec_snapshot.yaml
  batch_plan.json                  # full logical stage plan
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
          environment_plan.json
          before_steps.json
          launcher_plan.json
          runtime_probe.json
          logs/
          artifacts/
            artifact_manifest.json
            files/
          outputs/
            stage_outputs.json
```

Train/eval pipeline root:

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
```

## Invariants

- A Slurm array task maps to one user stage attempt.
- A stage attempt executes train or eval, never both.
- A stage attempt records one exit code.
- `planned` means a stage plan exists; `ready` lives in materialization status and means submit files are available.
- Input contract failures before submission are `blocked`, not attempts.
- Environment loading is explicit; generated sbatch uses `python -m slurmforge.executor.stage`, not a PATH-dependent helper binary.
- Runtime Python is explicit through `runtime.executor.python.bin` and `runtime.user.<name>.python.bin`; both are probed in full dry-run audits and executor attempts. A failed runtime probe is `runtime_contract_error` and blocks user code launch.
- Reusable `environments.<name>` modules, source scripts, and env vars run in the sbatch scope; the executor records the selected environment plan and applies environment env vars to user commands.
- User pre-commands are explicit through `before_steps`; user launch is explicit through `launcher_plan`; Slurm resources, environments, pre-commands, and user launch are separate concerns.
- GPU sizing is resolved before persistence. Config may use `resources.gpus_per_node: "auto"`, but every `StageInstancePlan.resources.gpus_per_node`, group plan, and sbatch file carries a numeric GPU count plus the explanatory `resource_sizing` payload.
- `resources.gpu_type` references `hardware.gpu_types`; hardware `slurm.constraint` is a scheduler default and is overridden by an explicit stage `resources.constraint`.
- `torchrun` launch declares `single_node` or `multi_node`; multi-node launch uses `srun` with explicit rendezvous values.
- Torchrun validation rejects resource mismatches before submission: `single_node` requires one Slurm node, explicit `nnodes` must match `resources.nodes`, `nproc_per_node` cannot exceed `resources.gpus_per_node`, and rendezvous ports must be valid.
- Artifact storage is explicit through `artifact_store_plan`; checkpoint passing never relies on a previous shell block.
- Artifact digest verification is enforced when `artifact_store_plan.verify_digest=true`; a mismatch is `artifact_integrity_error`.
- Required stage inputs are verified before sbatch generation and before executor launch; missing or unreadable inputs fail as `input_contract_error`.
- Train status and eval status are separate records.
- Train/eval pipeline execution is driven by short-lived control gates.
- Train/eval pipeline dependency progression is contract-driven: the control gate resolves the target stage's declared inputs from successful upstream stage outputs, not from train/eval-specific code paths.
- Workflow state is durable but orchestration-only; control gate job ids are recorded in `control/gate_ledger.json`, and submitted stage group job ids are recorded in each stage batch submission ledger.
- Stage submission is manifest-driven; submit code never glob-submits stale root-level sbatch files.
- `submissions/ledger.json` is the scheduler job-id source of truth for `train`, `eval`, `run`, `resubmit`, and `status --reconcile`.
- Public submit APIs are gated by `PreparedSubmission`; direct ledger mutation is not a supported submission path.
- User-facing submit paths are new-only. A batch with submitted scheduler job ids is not silently reused; create a new execution through `resubmit`.
- Pipeline recovery is the only path that may adopt already submitted groups and continue missing groups.
- The public `emit` API renders/writes control gate sbatch files and manifest-scoped stage submit files. Control jobs are recorded through `control/gate_ledger.json`; stage group sbatch files are emitted only through `submission.prepare_stage_submission`.
- Submission records each group job id immediately, can continue after partial submission, and fails safe only for the uncertain window where a group may have reached `sbatch` without a recorded job id.
- Batch notifications are Slurm finalizer jobs submitted by `submission.submit_stage_batch_finalizer` after terminal array groups with `afterany` dependencies. Large dependency sets are reduced through generated barrier jobs before the single notification job is submitted.
- Train/eval pipeline notifications are final-gate terminal actions. They send at most one `train_eval_pipeline_finished` summary per train/eval pipeline root and do not create separate train/eval batch notifications for pipeline-submitted stage batches.
- Notification delivery state is persisted under `notifications/records/<event>.email.json`; summary content is rendered from `NotificationSummaryInput`. Root snapshots and notification inputs are assembled by `root_model.notifications` and exposed through `notifications.read_model`, while `notifications.summary` only counts, formats, and renders the neutral summary input.
- `sforge status` is read-only by default; only `sforge status --reconcile` mutates status records from Slurm state.
- `status --reconcile` refreshes aggregate read models from stage records, including `run_status.json` and train/eval pipeline `train_eval_pipeline_status.json`.
- Stage status and attempt commits are per-stage writes; aggregate stage-batch and train/eval pipeline read models are refreshed by executor completion, control gate progression, notification finalizers, and `status --reconcile`.
- Scheduler observation merges active `squeue` rows with accounting `sacct` rows, so running tasks are visible before they reach Slurm accounting.
- Scheduler observations are append-only records in `scheduler_observations.jsonl`.
- Scheduler reconcile creates a scheduler-sourced attempt when Slurm reports a running or terminal task without an executor attempt.
- Successful checkpoint and artifact outputs are managed under the attempt artifact store and include digest plus producer attempt lineage.
- Metric and manifest outputs are first-class output refs; metrics carry extracted values, and manifests are managed as artifacts.
- `sforge estimate` uses the same planner as submission and returns the resource estimate without materializing batch roots.
- `--dry-run=json` and `--dry-run=full` produce machine-readable audits without materializing batch roots; full mode adds runtime probes, contract verification, and `resource_estimate`.
- GPU budget waves enforce `dispatch.max_available_gpus` across all concurrent resource groups.
- Partial train success keeps the full eval `batch_plan.json` and writes a selected execution subset separately.
- Slurm reconcile maps statuses by array task and waits through a missing-output grace period before marking `missing_attempt_result`.
- Resubmission targets one stage, reapplies overrides to the saved spec snapshot, validates the resulting spec, replans the target stage, resolves inputs from the new contract plus lineage, and reserves a new batch root before submit-file generation.
