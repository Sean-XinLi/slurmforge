# Internals

This document captures the internal contracts that keep planning, submission, execution, status, and resubmit aligned.

## Package Boundaries

- `spec.parse_sections` loads and assembles the top-level experiment spec; section parsers live under `spec.parse_*`.
- `spec.validation` orchestrates validation; resources, runtime, runs, launcher, inputs, outputs, notifications, and topology have separate validators.
- `config_contract` is the source for config fields, defaults, option values, workflow names, stage names, key registration, starter comments, starter config guides, and spec-facing validation helpers.
- `docs_render.config_reference` owns rendered config reference text from the `config_contract` catalog.
- `spec.run_paths`, `spec.run_ids`, and `spec.run_expansion` own run override path normalization, run identity generation, and `single`/`grid`/`cases`/`matrix` expansion. `spec.queries` is kept for non-run spec lookup helpers.
- `starter.config_yaml` is split by render concern: top-level orchestration, scalar quoting, top-level sections, and stage/input/output sections.
- `planner` has no package facade; callers import role modules such as `planner.stage_batch`, `planner.train_eval_pipeline`, `planner.sources`, `planner.audit`, `planner.resource_estimate`, and `planner.summaries` directly.
- `executor.stage` is the stage execution entrypoint. `executor.attempt` owns attempt/status transaction writes, `executor.runner` owns runtime checks, input verification, environment construction, and user command execution, and `executor.finalize` owns output discovery and final stage output records.
- `resolver.explicit` is split by source shape: external paths, producer stage batches, and producer run directories.
- `root_paths` is the single source for inferring a parent train/eval pipeline root from a stage batch root.
- `io.diagnostics` is the single traceback diagnostic writer used by executor, short-lived control gates, stage submission, and notification delivery.
- Public package facades are limited to `spec`, `starter`, `contracts`, `slurm`, and `io`. Internal packages keep empty facades and callers import role modules directly.
- Nested internal package facades are allowed only as explicit local subsystem entrypoints, for example `plans.serde`, `planner.payloads`, `emit.stage_render`, `outputs.discovery`, and `resolver.explicit`.
- `materialization` owns workflow-level materialization: persisting a planned root layout, seeding planned status records, refreshing root snapshots, and materializing sourced or selected stage batches.
- `storage` owns persisted layout, storage paths, batch materialization records, derived root reservation, source contracts, workflow/control files, execution indexes, and plan readers. It does not seed status records, refresh root read models, or own workflow-level materialization.
- `status` owns per-stage status/attempt records and scheduler reconciliation. Reconcile internals are split between workflow, attempt reconstruction, scheduler observations, and reconciliation rules.
- `root_model` owns root detection, root refs, run/pipeline aggregation, root snapshots, notification snapshots, and planned status seeding after storage layout is written.
- `control.workflow` owns train/eval pipeline progression through short-lived controller invocations. It reconciles stage instances, resolves dependency edges, dispatches ready instances, submits instance and catch-up gates, and records durable workflow state.

Persisted file shapes are specified in [Record Contract](record-contract.md).

## Contracts

Every consuming stage writes `input_bindings.json`. This file is the authoritative dependency contract; env vars and CLI flags are only injection mechanisms.

Executable submission validates required inputs before writing sbatch files. The verification result is written to `input_verification.json`; missing, unreadable, or digest-mismatched required paths fail with `input_contract_error` before a job is submitted.

The executor repeats input verification on the compute node before starting user code. If a checkpoint disappears after submission or no longer matches recorded lineage, the stage fails as `input_contract_error` and user code is not launched.

Every stage plan carries a schema-versioned output contract. Checkpoint discovery and artifact collection are parsed and validated before planning; the executor consumes that contract directly.

Every successful stage writes `stage_outputs.json`. Downstream stages consume explicit output refs, not process-local shell state. Output refs cover checkpoints, artifacts, metrics, and manifests.

## Materialization

Every stage batch writes `materialization_status.json`.

- `planned`: the plan exists.
- `verifying_inputs`: readiness checks are running.
- `ready`: submit files and ledger are available.
- `blocked`: the batch cannot be submitted because a contract check failed.

Submission is a gated contract. A submit call reloads the batch from disk and refuses to run unless materialization is `ready`, the generation id matches the ledger, the submit manifest exists, and submit-phase input verification reports are not failed.

User-facing submit paths are new-only. If the target batch already has scheduler job ids, submission fails and a new execution must go through `resubmit`.

## Lineage

Every stage batch writes `lineage_index.json`. It records stage instances, source roots, and resolved input sources consumed by the batch. This makes an eval batch root self-contained for audit and resubmit.

Each run directory writes `root_ref.json`. It records the containing stage batch root and, when present, the parent train/eval pipeline root for audit and root inference.

Successful outputs are managed through the configured artifact store. Checkpoints, manifests, metric sources, and declared artifacts record source and managed digests, the applied storage strategy, and producer attempt lineage.

## Ledger

Every stage batch has one submission ledger under `submissions/ledger.json`. `train`, `eval`, `run`, and `resubmit` write through this ledger, and `status --reconcile` uses it as the scheduler job-id source of truth.

Stage submission is manifest-based. The submitter only submits sbatch files listed in the current `submit_manifest.json`; it never glob-submits old `group_*.sbatch` files.

Submission is per-group and recoverable inside the stage submission ledger. The ledger records a group-submitting marker before `sbatch`, records each returned job id immediately, adopts recorded groups during recovery, and continues missing groups. If the process dies while a group may have reached `sbatch` without a recorded job id, restart fails safe.

## Control Plane Ownership

Train/eval pipelines use short-lived control jobs, not a long-running orchestration allocation.

`control/control_plan.json` is the static control plan used by gate jobs.

`control/workflow_state.json` is the durable workflow state machine for train/eval progression.

`control/workflow_status.json` is the typed status read model for users and `sforge status`. Its `control_jobs` projection includes every control submission record, including `failed` and `uncertain` records without scheduler job ids, so operators can see why a gate or notification did not produce a Slurm job.

`control/control_submissions.json` is the authoritative ledger for stage-instance gates, dispatch catch-up gates, and terminal notification control jobs. It records `submitting` before `sbatch`, `submitted` after scheduler job ids are known, `failed` when no scheduler job was created and retry is safe, and `uncertain` when a retry would risk duplicate control jobs. Records are self-validating: kind, state, target key, sbatch paths, scheduler job ids, and failure reasons must agree before the ledger can be read as a valid control state.

`execution/stage_catalog.json` is the catalog of planned pipeline stage batch roots. It includes train and eval stage plans even when eval is not yet submitted, so resubmit and inspection commands can reason about the full declared pipeline.

`execution/runtime_batches.json` is the runtime registry of batch roots that participate in the current execution. It starts with the train entry batch and gains one dispatch batch for each submitted downstream projection. Status and reconcile read this registry instead of recursively scanning `stage_batches/**`.

Each controller invocation reconciles runtime status, resolves dependency edges from successful upstream `stage_outputs.json`, materializes only the ready downstream dispatch projection, and marks unresolved required inputs as `blocked`.

Status rendering is split into read-model construction and formatting. `orchestration.status_read_model` owns root detection, optional reconcile, workflow status loading, and aggregate status collection. `orchestration.status_format` owns CLI text rendering. `orchestration.status_view` is only the thin public wrapper.

## Notifications

Notification summaries are derived from the same status read models as `sforge status`.

Direct `sforge train`, `sforge eval`, and `sforge resubmit` submit one Slurm mail notification job after terminal stage array groups and send one batch summary when configured.

`sforge run` sends one train/eval pipeline summary from terminal aggregation after the full pipeline reaches a terminal state. Terminal aggregation stores the workflow terminal summary and the notification control key; scheduler job ids live in `control/control_submissions.json`. Pipeline-submitted stage batches do not send separate batch summaries unless they are submitted directly.
