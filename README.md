# slurmforge

`slurmforge` is a Slurm-native stage-batch pipeline system for AI training and evaluation workflows.

The current command model is deliberately small:

```bash
sforge validate --config experiment.yaml
sforge plan train --config experiment.yaml --dry_run=full --output plan.audit.json
sforge plan eval --config experiment.yaml --checkpoint /path/to/model.pt --input-name model_input
sforge plan run --config experiment.yaml
sforge train --config experiment.yaml --dry_run
sforge train --config experiment.yaml --dry_run=json
sforge train --config experiment.yaml --emit_only
sforge train --config experiment.yaml
sforge eval --config experiment.yaml --checkpoint /path/to/step_12000.pt
sforge eval --config experiment.yaml --checkpoint /path/to/model.pt --input-name model_input
sforge eval --config experiment.yaml --from-train-batch /path/to/train_batch
sforge run --config experiment.yaml
sforge status --from /path/to/root
sforge status --from /path/to/root --reconcile
sforge resubmit --from /path/to/root --stage eval --query state=failed
```

## Model

- `sforge train` submits train stage batches only.
- `sforge eval` submits eval stage batches only.
- `sforge run` submits a pipeline controller that advances stages in order.
- `sforge plan` compiles plans and sbatch files without submitting.
- `sforge status` reads persisted stage-level status. Add `--reconcile` to query Slurm through submission ledgers and update status before printing.
- `sforge resubmit` provides one stage-level command for exact re-executions, failed-stage retries, and override-based follow-up runs.

The execution atom is a **stage attempt**. One Slurm array task maps to one user stage attempt, and one attempt records one stage exit code.

The programmatic submission boundary is also stage-batch based. External callers prepare a batch with `prepare_stage_submission`, submit only the returned `PreparedSubmission` with `submit_prepared_stage_batch`, and read or reconcile through read-only submission APIs. Ledger mutation and stage sbatch generation helpers are internal implementation details.

Execution mode is uniform across commands:

- `--dry_run` previews the plan only. It writes no files and submits nothing.
- `--dry_run=json` writes a machine-readable plan audit to stdout or `--output`.
- `--dry_run=full` adds contract verification and runtime probes to the audit. Deferred upstream inputs in a pipeline preview stay deferred instead of failing the whole plan.
- `--emit_only` writes plan, sbatch, manifests, and a planned submission ledger, but submits nothing.
- No execution-mode flag writes files and submits.
- `sforge plan train|eval|run` defaults to emit mode; `--dry_run` is preview-only.

## Config Shape

Configs are stage-driven:

```yaml
project: "demo_project"
experiment: "finetune_v2"

storage:
  root: "./runs"

runtime:
  executor:
    python:
      bin: "python3.11"
      min_version: "3.10"
    module: "slurmforge.executor.stage"
    bootstrap:
      scope: "sbatch"
      steps:
        - type: module_load
          name: "cuda/12.1"
        - type: source
          path: "/shared/miniconda/bin/activate"
          args: ["myenv"]
    env:
      HF_HOME: "/shared/hf"
  user:
    default:
      python:
        bin: "python3.11"
        min_version: "3.10"

artifact_store:
  strategy: "copy"
  fallback_strategy: null
  verify_digest: true
  fail_on_verify_error: true

matrix:
  axes:
    train.entry.args.lr: [1e-4, 5e-5]

stages:
  train:
    enabled: true
    entry:
      type: python_script
      script: "train.py"
      workdir: "."
      args:
        epochs: 3
    launcher:
      type: torchrun
      mode: single_node
      nnodes: 1
      nproc_per_node: 8
      rendezvous:
        backend: c10d
        endpoint: auto
        port: 29500
    resources:
      partition: "gpu"
      nodes: 1
      gpus_per_node: 8
      cpus_per_task: 12
      mem: "256G"
    outputs:
      checkpoint:
        kind: file
        required: true
        discover:
          globs: ["checkpoints/**/*.pt"]
          select: "latest_step"
      logs:
        kind: files
        discover:
          globs: ["logs/**/*.log"]

  eval:
    enabled: true
    depends_on: "train"
    entry:
      type: python_script
      script: "eval.py"
      workdir: "."
      args:
        split: "test"
    launcher:
      type: single
    inputs:
      checkpoint:
        source:
          kind: upstream_output
          stage: train
          output: checkpoint
        expects: path
        required: true
        inject:
          flag: "checkpoint_path"
          env: "SFORGE_INPUT_CHECKPOINT"
    resources:
      partition: "gpu"
      nodes: 1
      gpus_per_node: 1
      cpus_per_task: 8
      mem: "32G"
    outputs:
      accuracy:
        kind: metric
        file: "eval/metrics.json"
        json_path: "$.accuracy"
        required: true
      eval_report:
        kind: manifest
        file: "eval/manifest.json"
        required: false

dispatch:
  max_available_gpus: 32
  overflow_policy: "serialize_groups"
```

`matrix.axes` may address stage paths directly, such as `train.entry.args.lr`; this is normalized to `stages.train.entry.args.lr`.

Stage-batch v1 intentionally supports only `stages.train` and `stages.eval`. `eval` may depend on `train`; no other topology is accepted.

Stage inputs are resolved by the contract kernel, not by CLI-specific code paths. `sforge run` resolves structured `source.kind=upstream_output` inputs from successful upstream outputs. `sforge eval --checkpoint`, `--from-run`, and `--from-train-batch` are source shortcuts that bind the selected eval input; use `--input-name` when the eval stage has multiple inputs.

`checkpoint` is a conventional input/output name, not a required eval schema field. A config may use another input name as long as the input declares the upstream output source and injection contract explicitly.

Runtime bootstrap and user launcher are separate contracts. The generated sbatch bootstraps `runtime.executor`, then calls `python -m slurmforge.executor.stage`; the executor uses `runtime.user.<name>` and the stage launcher to run user code.

`runtime.executor.python.bin` is the executor Python contract. `runtime.user.<name>.python.bin` is the user-stage Python contract. Both are recorded in plans, checked by `--dry_run=full`, and written by the executor to each attempt's `RuntimeContractReport` in `runtime_probe.json`. Executor attempts treat runtime contract failures as `runtime_contract_error` and do not launch user code.

The persisted runtime plan keeps the same shape:

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

`runtime.executor.bootstrap.scope` is currently `sbatch`. Bootstrap steps run once in the generated sbatch; user runtime env is injected by the executor.

`torchrun` launcher mode is explicit. `single_node` runs one local `torch.distributed.run`; `multi_node` uses `srun` to start one torchrun launcher per node with explicit rendezvous settings.

Torchrun validation is intentionally strict: `single_node` must request exactly one Slurm node, explicit `nnodes` must match `resources.nodes`, `nproc_per_node` cannot exceed `resources.gpus_per_node`, and rendezvous ports must be valid TCP ports.

`artifact_store.strategy` controls how produced checkpoints and declared artifacts are managed under each attempt. Supported strategies are `copy`, `hardlink`, `symlink`, and `register_only`; optional `fallback_strategy` handles filesystems that cannot apply the preferred strategy. When `verify_digest` is true, managed outputs are re-hashed after storage; digest mismatch fails the stage when `fail_on_verify_error` is true.

GPU budget is global per stage batch. The planner packs resource groups into deterministic budget waves and writes wave dependencies so `dispatch.max_available_gpus` is not exceeded by combined array throttles.

## Storage

A stage batch root contains:

```text
<batch_root>/
  manifest.json
  lineage_index.json
  materialization_status.json
  spec_snapshot.yaml
  batch_plan.json
  selected_batch_plan.json
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
    groups.json
    gpu_budget_plan.json
    selected_groups.json
    selected_gpu_budget_plan.json
  runs/
    <run_id>/
      root_ref.json
      stage_plan.json
      input_bindings.json
      input_verification.json
      stage_outputs.json
      status.json
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

A pipeline root contains:

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
    events.jsonl
    controller.sbatch
  stage_batches/
    train/
    eval/
```

## Contracts

Every consuming stage writes `input_bindings.json`. This file is the authoritative dependency contract; env vars and CLI flags are only injection mechanisms.

Pipeline progression resolves inputs through the same contract. The controller reads the next stage's declared inputs, binds `upstream_output` and runtime-supplied pipeline inputs from successful upstream `stage_outputs.json`, materializes the selected execution subset, and marks unresolved required inputs as `blocked`.

Every stage batch writes `materialization_status.json`. `planned` means the plan exists, `verifying_inputs` means readiness checks are running, `ready` means submit files and ledger are available, and `blocked` means the batch cannot be submitted because a contract check failed.

Executable submission validates required inputs before writing sbatch files. The verification result is written to `input_verification.json`; missing, unreadable, or digest-mismatched required input paths fail with `input_contract_error` before a job is submitted.

The executor repeats the same verification on the compute node before starting user code. If a checkpoint is deleted after submission, is not visible from the node, or no longer matches the expected digest recorded in lineage, the stage fails as `input_contract_error` and the user script is not launched.

Every stage batch writes `lineage_index.json`. It records stage instances, source roots, and the resolved input sources consumed by the batch. This makes an eval batch root self-contained for audit and resubmit: it can replan eval and still trace the train checkpoint producer without relying on an external directory convention.

Every stage plan carries a schema-versioned typed output contract. Checkpoint discovery and artifact collection are parsed and validated before planning; the executor consumes that contract directly instead of interpreting ad-hoc output dictionaries.

Every successful stage writes `stage_outputs.json`. Downstream stages consume explicit output refs, not process-local shell state. Output refs are first-class for checkpoints, artifacts, metrics, and manifests. Metrics are extracted from declared JSON files, keep their scalar value in the output ref, and the metric source file is managed through the artifact store with digests.

Successful outputs are managed through the configured artifact store. Checkpoints, manifests, metric sources, and declared artifacts get source and managed digests, record the applied storage strategy, and are referenced from `stage_outputs.json` with producer stage and attempt lineage.

Each run directory writes `root_ref.json`. It records the containing stage batch root and, when present, the parent pipeline root for audit and root inference. Stage status commits stay per-stage; layout writes, controller progression, and `status --reconcile` refresh aggregate `run_status.json` / `pipeline_status.json` from stage records.

Resubmit is replan-based and lineage-aware. It selects runs from an existing root, applies `--set` overrides to the saved spec snapshot, recompiles only the requested stage into a new stage batch root, then resolves inputs from the new spec contract plus the saved lineage index. It does not blindly copy old input bindings, but it can use prior resolved inputs when the spec declares a runtime-supplied source.

Every materialized resubmit writes `source_plan.json` and `source_lineage.json` before submit-file generation. The derived batch root is reserved as a new directory under `<source_root>/derived_batches/`, so repeated resubmits never mutate an earlier batch.

Stage status is monotonic. A late submit marker cannot overwrite a terminal `success`, `failed`, `cancelled`, or `blocked` result. The controller persists orchestration state only; stage scheduler job ids and submitted group facts live in the stage batch submission ledger.

Status aggregation is derived from stage records. Layout writes, controller completion, and `status --reconcile` refresh `run_status.json`; pipeline roots also refresh `pipeline_status.json`.

Controller files have separate ownership. `controller_job.json` is an immutable submit fact for the controller Slurm job: pipeline id, scheduler job id, submit time, and sbatch path. It is written once after a successful `sbatch` call and never contains mutable fields such as `state`, `reason`, `scheduler_state`, or `scheduler_exit_code`. `controller_status.json` is the only mutable controller scheduler/runtime status file. `controller_state.json` is the mutable orchestration state machine for pipeline progression.

Stage submission is manifest-based. The submitter only submits the sbatch files listed in the current `submit_manifest.json`; it never glob-submits old `group_*.sbatch` files left in the root.

Every stage batch has one submission ledger under `submissions/ledger.json`. `train`, `eval`, `run`, and `resubmit` write through this ledger, and `status --reconcile` uses it as the scheduler job-id source of truth.

Submission is a gated contract, not a bag of helper functions. A submit call reloads the batch from disk and refuses to run unless `materialization_status.json` is `ready`, the generation id matches the ledger, the submit manifest exists, and submit-phase `input_verification.json` files are not failed. User-facing submit paths are new-only: if the target batch already has scheduler job ids, submission fails and a new execution must go through `resubmit`. The public `emit` package only exposes controller render/write helpers; controller jobs are recorded through `controller_job.json`, and stage sbatch emitters are private and are called only by `submission.prepare_stage_submission`.

Submission is per-group and recoverable inside the controller. The ledger records a group-submitting marker before `sbatch`, records each returned job id immediately, adopts recorded groups during explicit controller recovery, and continues missing groups. If the process dies while a group may have reached `sbatch` without a recorded job id, restart fails safe.

Slurm reconcile observes both `sacct` and `squeue`. `squeue` covers active jobs that have not reached accounting yet; `sacct` covers terminal history. Observations are appended to `scheduler_observations.jsonl`.

Slurm reconcile has a missing-output grace period. A completed Slurm job without `stage_outputs.json` remains in a waiting reconcile state briefly before it is classified as `missing_attempt_result`.

Scheduler reconcile also creates a scheduler-sourced attempt record when Slurm reports a running or terminal task that did not leave an executor attempt. This keeps the task-to-attempt audit trail intact even when Python never started.

## Development

Install from source:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Run validation:

```bash
PYTHONPATH=src python3.11 -m pytest -q
PYTHONPATH=src lint-imports --config .importlinter
```
