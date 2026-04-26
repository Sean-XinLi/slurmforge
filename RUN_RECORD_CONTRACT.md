# Stage Batch Record Contract

This document defines the current persisted execution contract for `slurmforge`.

## Planning Objects

The core planning objects are:

- `RunDefinition`
- `StageInstancePlan`
- `StageBatchPlan`
- `PipelinePlan`
- `InputBinding`
- `OutputRef`

`RunDefinition` is the matrix-expanded run identity. Each matrix instance has one stable `run_id`.

`StageInstancePlan` is the executable plan for one stage of one run:

- `stage_instance_id = <run_id>.<stage_name>`
- `run_id`
- `stage_name`
- `stage_kind`
- `entry`
- `resources`
- `runtime_plan`
- `launcher_plan`
- `artifact_store_plan`
- `input_bindings`
- `output_contract`
- `lineage`

`StageBatchPlan` groups same-stage instances by resource shape and emits Slurm array jobs. A group never mixes train and eval.

`PipelinePlan` is orchestration metadata. It is consumed by the controller and does not execute user code directly.

The supported topology is deliberately narrow: `train`, `eval`, or `train -> eval`. Stage-batch v1 is not an arbitrary DAG engine.

## Runtime Objects

Runtime state is stage-scoped:

- `StageAttemptRecord`
- `StageStatusRecord`
- `RunStatusRecord`
- `PipelineStatusRecord`

`StageAttemptRecord` records one stage attempt and one stage exit code:

- `attempt_id`
- `stage_instance_id`
- `attempt_source = executor | scheduler_reconcile`
- `attempt_state = starting | running | final | reconciled`
- `scheduler_job_id`
- `scheduler_array_job_id`
- `scheduler_array_task_id`
- `scheduler_state`
- `scheduler_exit_code`
- `node_list`
- `started_by_executor`
- `executor_started_at`
- `executor_finished_at`
- `started_at`
- `finished_at`
- `exit_code`
- `failure_class`
- `reason`
- `log_paths`
- `artifact_paths`
- `artifact_manifest_path`

`StageStatusRecord` is the atomic status record:

- `state = planned | queued | running | success | failed | cancelled | skipped | blocked`
- `latest_attempt_id`
- `latest_output_digest`
- `failure_class`
- `reason`

Run and pipeline status are derived from stage status records.

Status writes use monotonic transitions. Normal queued/running markers cannot move a terminal stage back to a non-terminal state.

Every status transition appends a line to `status_events.jsonl` in the run directory.

Every materialized run directory writes `root_ref.json`. It records the containing stage batch root and, when applicable, the parent pipeline root. Stage status commits stay per-stage; layout writes, controller progression, and `status --reconcile` refresh aggregate `run_status.json` / `pipeline_status.json` from the current stage records.

Every executor attempt writes `runtime_probe.json` under the attempt directory. It is a `RuntimeContractReport` with a single `state`, `failure_reason`, and both executor and user Python probes. A failed report prevents user code launch.

Runtime plans use the same nested runtime shape as the spec:

```json
{
  "executor": {
    "python": {
      "bin": "python3.11",
      "min_version": "3.10"
    },
    "module": "slurmforge.executor.stage"
  },
  "user": {
    "name": "default",
    "python": {
      "bin": "python3.11",
      "min_version": "3.10"
    }
  }
}
```

## Input Contract

Every stage instance has `input_bindings.json`:

```json
{
  "schema_version": 1,
  "stage_instance_id": "run_001.eval",
  "bindings": {
    "checkpoint": {
      "source": {
        "kind": "upstream_output",
        "stage": "train",
        "output": "checkpoint"
      },
      "expects": "path",
      "resolved": {
        "kind": "path",
        "path": "/abs/path/checkpoints/step_12000.pt",
        "digest": "sha256..."
      },
      "inject": {
        "flag": "checkpoint_path",
        "env": "SFORGE_INPUT_CHECKPOINT",
        "mode": "path"
      },
      "resolution": {
        "kind": "upstream_output",
        "producer_root": "/abs/path/train_batch",
        "producer_run_dir": "/abs/path/train_batch/runs/run_001",
        "producer_stage_instance_id": "run_001.train",
        "producer_run_id": "run_001",
        "producer_stage_name": "train",
        "output_name": "checkpoint",
        "output_path": "/abs/path/checkpoints/step_12000.pt",
        "output_digest": "sha256...",
        "selection_reason": "latest_step"
      }
    }
  }
}
```

The binding file is the source of truth. Environment variables and CLI flags are derived injection forms. `checkpoint` is only a conventional name in examples; eval may use any input name if the source and injection contract are explicit.

The YAML input contract is explicit:

```yaml
inputs:
  checkpoint:
    source:
      kind: upstream_output
      stage: train
      output: checkpoint
    expects: path
    required: true
    inject:
      flag: checkpoint_path
      env: SFORGE_INPUT_CHECKPOINT
```

Allowed source kinds are `upstream_output` and `external_path`. CLI shortcuts such as `--checkpoint` compile to `external_path` bindings with `resolution.source_role=checkpoint`; checkpoint is not a separate core source kind.

`resolved` is the typed resolved value consumed by injection and verification. It may be `path`, `value`, `manifest`, or `unresolved`. For `path` and `manifest`, `resolved.path` is the injected filesystem value. For `value`, `resolved.value` is injected by `mode: value` or serialized by `mode: json`. `resolution` is the audit trail for how the binding was resolved: producer root, producer stage instance, output name, digest, and any lineage lookup used to recover the source.

## Materialization Contract

Every stage batch root writes `materialization_status.json`:

```json
{
  "schema_version": 1,
  "batch_id": "eval_batch_xxx",
  "stage_name": "eval",
  "state": "ready",
  "failure_class": null,
  "reason": "",
  "verified_at": "2026-01-01T00:00:00+00:00",
  "submit_manifest_path": "/abs/path/submit/submit_manifest.json"
}
```

States:

- `planned`: batch layout and logical plans exist.
- `verifying_inputs`: required input readiness checks are running.
- `ready`: submit files and submission ledger are available.
- `blocked`: the batch cannot be submitted because a contract check failed.

`blocked` is readiness failure, not execution failure. It does not create an attempt. The affected stage instances move to `StageStatusRecord.state=blocked` with `failure_class=input_contract_error`.

Submission has one public write path: `prepare_stage_submission(batch)` returns a `PreparedSubmission`, and `submit_prepared_stage_batch(prepared)` is the only public submit entrypoint. Low-level ledger writes, event appends, and stage sbatch generation are private package internals.

## Input Verification Contract

Every executable stage instance writes `input_verification.json` before sbatch generation and again before executor launch.

```json
{
  "schema_version": 1,
  "stage_instance_id": "run_001.eval",
  "run_id": "run_001",
  "stage_name": "eval",
  "phase": "submit",
  "state": "verified",
  "records": [
    {
      "input_name": "checkpoint",
      "source": {
        "kind": "upstream_output",
        "stage": "train",
        "output": "checkpoint"
      },
      "expects": "path",
      "resolved_kind": "path",
      "resolved_path": "/abs/path/checkpoints/step_12000.pt",
      "required": true,
      "path_kind": "file",
      "exists": true,
      "readable": true,
      "size_bytes": 123,
      "expected_digest": "sha256...",
      "producer_digest": "sha256...",
      "digest": "sha256...",
      "value_digest": "",
      "phase": "submit",
      "state": "verified",
      "failure_class": null,
      "reason": "verified"
    }
  ]
}
```

Verification phases:

- `submit`: runs before executable sbatch files and submission ledger are generated.
- `executor`: runs on the compute node before user code is launched.

Required input rules:

- required inputs must satisfy the typed `resolved` contract: `path`/`manifest` need `resolved.path`; `value` needs `resolved.kind=value`;
- filesystem-backed inputs must exist;
- filesystem-backed inputs must be readable;
- inputs with `expected_digest` must match the current file digest;
- `upstream_output` and `external_path` default to file verification;
- failure is `input_contract_error`;
- executor preflight failure must not launch user code.

Normal submit/executor verification records `expected_digest` from lineage when available. When an expected digest is present, verification hashes the current file and rejects mismatches as `input_contract_error`.

## Lineage Contract

Every stage batch and pipeline root writes `lineage_index.json`.

Stage batch lineage records:

- `batch_id`
- `stage_name`
- `source_ref`
- `run_ids`
- `stage_instances`
- `source_roots`
- `input_sources`

`input_sources` repeats each persisted input binding with its `resolution`. This is the durable join point between an eval batch and the train batch that produced its checkpoint.

Pipeline lineage records:

- `pipeline_id`
- `stage_order`
- `run_ids`
- `stage_batches`
- `source_roots`

Lineage is used by `resubmit` after replanning the target stage. For example, `sforge resubmit --from <eval_batch_root> --stage eval` can resolve the checkpoint from the eval batch's own lineage index even when the train batch root is not the command target.

Each materialized resubmit reserves a fresh `<source_root>/derived_batches/<batch_id>` directory and writes `source_plan.json` plus `source_lineage.json` before input verification or sbatch generation. A failed readiness check leaves an auditable source contract, not a hidden half-plan.

## Output Contract

Every `stage_plan.json` carries a schema-versioned typed `output_contract`. Checkpoints are conventional file outputs with discovery globs and a selection policy; declared multi-file outputs, metrics, and manifests are represented by first-class output contracts. Executors consume this typed contract directly.

Every successful stage instance has `stage_outputs.json`:

```json
{
  "schema_version": 1,
  "stage_instance_id": "run_001.train",
  "outputs": {
    "checkpoint": {
      "kind": "file",
      "path": "/abs/path/attempts/0001/artifacts/files/abc123_step_12000.pt",
      "source_path": "/abs/path/checkpoints/step_12000.pt",
      "managed": true,
      "strategy": "copy",
      "digest": "sha256...",
      "source_digest": "sha256...",
      "managed_digest": "sha256...",
      "verified": true,
      "producer_stage_instance_id": "run_001.train",
      "producer_attempt_id": "0001",
      "selection_reason": "latest_step"
    },
    "accuracy": {
      "kind": "metric",
      "path": "/abs/path/attempts/0001/artifacts/files/abc123_metrics.json",
      "source_path": "/abs/path/eval/metrics.json",
      "managed": true,
      "digest": "sha256...",
      "source_digest": "sha256...",
      "managed_digest": "sha256...",
      "verified": true,
      "value": 0.98,
      "producer_stage_instance_id": "run_001.eval",
      "producer_attempt_id": "0001",
      "selection_reason": "json_path:$.accuracy"
    },
    "eval_report": {
      "kind": "manifest",
      "path": "/abs/path/attempts/0001/artifacts/files/eval_manifest.json",
      "source_path": "/abs/path/eval/manifest.json",
      "managed": true,
      "producer_stage_instance_id": "run_001.eval",
      "producer_attempt_id": "0001",
      "selection_reason": "manifest_file"
    }
  },
  "artifacts": ["/abs/path/attempts/0001/artifacts/files/def456_train.log"],
  "artifact_manifest": "/abs/path/attempts/0001/artifacts/artifact_manifest.json"
}
```

Downstream stages consume explicit `OutputRef` values resolved into `InputBinding` records.

Every successful attempt writes an artifact manifest:

```json
{
  "schema_version": 1,
  "stage_instance_id": "run_001.train",
  "attempt_id": "0001",
  "artifacts": [
    {
      "name": "checkpoint",
      "kind": "file",
      "source_path": "/abs/path/checkpoints/step_12000.pt",
      "managed_path": "/abs/path/attempts/0001/artifacts/files/abc123_step_12000.pt",
      "strategy": "copy",
      "managed": true,
      "digest": "sha256...",
      "source_digest": "sha256...",
      "managed_digest": "sha256...",
      "verified": true,
      "size_bytes": 123,
      "optional": false
    }
  ]
}
```

## Storage Layout

Stage batch root:

```text
<batch_root>/
  manifest.json
  lineage_index.json
  materialization_status.json
  spec_snapshot.yaml
  batch_plan.json                  # full logical stage plan
  selected_batch_plan.json         # optional execution subset
  blocked_runs.json
  submit/
    submit_manifest.json
    submit.sh
    generations/
      gen_<digest>/
        group_001.sbatch
        submit.sh
    logs/
      gen_<digest>/
  submissions/
    ledger.json
    events.jsonl
  scheduler_observations.jsonl
  groups/
    gpu_budget_plan.json
    selected_gpu_budget_plan.json
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
          launcher_plan.json
          runtime_probe.json
          logs/
          artifacts/
            artifact_manifest.json
            files/
          outputs/
            stage_outputs.json
```

Pipeline root:

```text
<pipeline_root>/
  manifest.json
  lineage_index.json
  spec_snapshot.yaml
  pipeline_plan.json
  pipeline_status.json
  controller/
    controller_plan.json
    controller_job.json
    controller_state.json
    controller_status.json
    controller.sbatch
    events.jsonl
  stage_batches/
```

## Invariants

- A Slurm array task maps to one user stage attempt.
- A stage attempt executes train or eval, never both.
- A stage attempt records one exit code.
- `planned` means a stage plan exists; `ready` lives in materialization status and means submit files are available.
- Input contract failures before submission are `blocked`, not attempts.
- Runtime bootstrap is explicit; generated sbatch uses `python -m slurmforge.executor.stage`, not a PATH-dependent helper binary.
- Runtime Python is explicit through `runtime.executor.python.bin` and `runtime.user.<name>.python.bin`; both are probed in full dry-run audits and executor attempts. A failed runtime probe is `runtime_contract_error` and blocks user code launch.
- Runtime bootstrap runs in the sbatch scope only; the executor does not re-run module or activation steps inside the user stage shell.
- User launch is explicit through `launcher_plan`; Slurm resources, runtime bootstrap, and user launch are separate concerns.
- `torchrun` launch declares `single_node` or `multi_node`; multi-node launch uses `srun` with explicit rendezvous values.
- Torchrun validation rejects resource mismatches before submission: `single_node` requires one Slurm node, explicit `nnodes` must match `resources.nodes`, `nproc_per_node` cannot exceed `resources.gpus_per_node`, and rendezvous ports must be valid.
- Artifact storage is explicit through `artifact_store_plan`; checkpoint passing never relies on a previous shell block.
- Artifact digest verification is enforced when `artifact_store_plan.verify_digest=true`; a mismatch is `artifact_integrity_error`.
- Required stage inputs are verified before sbatch generation and before executor launch; missing or unreadable inputs fail as `input_contract_error`.
- Train status and eval status are separate records.
- Pipeline execution is controller-driven orchestration.
- Pipeline dependency progression is contract-driven: the controller resolves the target stage's declared inputs from successful upstream stage outputs, not from train/eval-specific code paths.
- Controller state is durable but orchestration-only; the controller Slurm job is recorded in `controller_job.json`, and submitted stage group job ids are recorded in each stage batch submission ledger.
- Stage submission is manifest-driven; submit code never glob-submits stale root-level sbatch files.
- `submissions/ledger.json` is the scheduler job-id source of truth for `train`, `eval`, `run`, `resubmit`, and `status --reconcile`.
- Public submit APIs are gated by `PreparedSubmission`; direct ledger mutation is not a supported submission path.
- User-facing submit paths are new-only. A batch with submitted scheduler job ids is not silently reused; create a new execution through `resubmit`.
- Controller recovery is the only path that may adopt already submitted groups and continue missing groups.
- The public `emit` API only renders/writes the controller sbatch. Controller jobs are recorded through `controller_job.json`; stage sbatch files are emitted only through `submission.prepare_stage_submission`.
- Submission records each group job id immediately, can continue after partial submission, and fails safe only for the uncertain window where a group may have reached `sbatch` without a recorded job id.
- `sforge status` is read-only by default; only `sforge status --reconcile` mutates status records from Slurm state.
- `status --reconcile` refreshes aggregate read models from stage records, including `run_status.json` and pipeline `pipeline_status.json`.
- Stage status and attempt commits are per-stage writes; aggregate stage-batch and pipeline read models are refreshed by layout writes, controller progression, and `status --reconcile`.
- Scheduler observation merges active `squeue` rows with accounting `sacct` rows, so running tasks are visible before they reach Slurm accounting.
- Scheduler observations are append-only records in `scheduler_observations.jsonl`.
- Scheduler reconcile creates a scheduler-sourced attempt when Slurm reports a running or terminal task without an executor attempt.
- Successful checkpoint and artifact outputs are managed under the attempt artifact store and include digest plus producer attempt lineage.
- Metric and manifest outputs are first-class output refs; metrics carry extracted values, and manifests are managed as artifacts.
- `--dry_run=json` and `--dry_run=full` produce machine-readable audits without materializing batch roots; full mode adds runtime probes and contract verification.
- GPU budget waves enforce `dispatch.max_available_gpus` across all concurrent resource groups.
- Partial train success keeps the full eval `batch_plan.json` and writes a selected execution subset separately.
- Slurm reconcile maps statuses by array task and waits through a missing-output grace period before marking `missing_attempt_result`.
- Resubmission targets one stage, reapplies overrides to the saved spec snapshot, validates the resulting spec, replans the target stage, resolves inputs from the new contract plus lineage, and reserves a new batch root before submit-file generation.
