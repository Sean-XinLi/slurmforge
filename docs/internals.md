# Internals

This document captures the internal contracts that keep planning, submission, execution, status, and resubmit aligned.

## Package Boundaries

- `spec.parse_sections` loads and assembles the top-level experiment spec; section parsers live under `spec.parse_*`.
- `spec.validation` orchestrates validation; resources, runtime, runs, launcher, inputs, outputs, notifications, and topology have separate validators.
- `config_contract` is the source for config defaults, option values, workflow names, stage names, and shared contract helpers consumed by schema, spec parsing, starter templates, and validation.
- `config_schema` owns field metadata, key registration, starter comments, starter config guides, and global config docs. Spec parsers use its key registry for unknown-key checks, but option/default values live in `config_contract`.
- `spec.run_paths`, `spec.run_ids`, and `spec.run_expansion` own run override path normalization, run identity generation, and `single`/`grid`/`cases`/`matrix` expansion. `spec.queries` is kept for non-run spec lookup helpers.
- `starter.config_yaml` is split by render concern: top-level orchestration, scalar quoting, top-level sections, and stage/input/output sections.
- `planner` has no package facade; callers import role modules such as `planner.stage_batch`, `planner.train_eval_pipeline`, `planner.sources`, `planner.audit`, `planner.resource_estimate`, and `planner.summaries` directly.
- `executor.stage` is the stage execution entrypoint. `executor.attempt` owns attempt/status transaction writes, `executor.runner` owns runtime checks, input verification, environment construction, and user command execution, and `executor.finalize` owns output discovery and final stage output records.
- `resolver.explicit` is split by source shape: external paths, producer stage batches, and producer run directories.
- `root_paths` is the single source for inferring a parent train/eval pipeline root from a stage batch root.
- `io.diagnostics` is the single traceback diagnostic writer used by executor, controller submission, stage submission, and notification delivery.
- Public package facades are limited to `spec`, `starter`, `contracts`, `slurm`, and `io`. Internal packages keep empty facades and callers import role modules directly.
- Nested internal package facades are allowed only as explicit local subsystem entrypoints, for example `plans.serde`, `planner.payloads`, `emit.stage_render`, `outputs.discovery`, and `resolver.explicit`.
- `materialization` owns workflow-level materialization: persisting a planned root layout, seeding planned status records, refreshing root snapshots, and materializing sourced or selected stage batches.
- `storage` owns persisted layout, storage paths, batch materialization records, derived root reservation, source contracts, controller files, and plan readers. It does not seed status records, refresh root read models, or own workflow-level materialization.
- `status` owns per-stage status/attempt records and scheduler reconciliation. Reconcile internals are split between workflow, attempt reconstruction, scheduler observations, and reconciliation rules.
- `root_model` owns root detection, root refs, run/pipeline aggregation, root snapshots, notification snapshots, and planned status/controller seeding after storage layout is written.
- `controller.stage_selection` owns controller-time selection of downstream stage runs from upstream outputs. The controller state machine records progression; it does not own storage persistence primitives.

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

Submission is per-group and recoverable inside the controller. The ledger records a group-submitting marker before `sbatch`, records each returned job id immediately, adopts recorded groups during recovery, and continues missing groups. If the process dies while a group may have reached `sbatch` without a recorded job id, restart fails safe.

## Controller Ownership

Controller files have separate ownership.

`controller_job.json` is an immutable submit fact for the controller Slurm job: train/eval pipeline id, scheduler job id, submit time, and sbatch path.

`controller_status.json` is the mutable controller scheduler/runtime status file.

`controller_state.json` is the mutable orchestration state machine for train/eval pipeline progression.

The controller resolves the next stage's declared inputs, binds `upstream_output` and runtime-supplied pipeline inputs from successful upstream `stage_outputs.json`, materializes the selected execution subset, and marks unresolved required inputs as `blocked`.

## Notifications

Notification summaries are derived from the same status read models as `sforge status`.

Direct `sforge train`, `sforge eval`, and `sforge resubmit` submit one Slurm finalizer job after terminal stage array groups and send one batch summary when configured.

`sforge run` sends one train/eval pipeline summary from the controller after the full pipeline reaches a terminal state. Controller-submitted stage batches do not send separate batch summaries unless they are submitted directly.
