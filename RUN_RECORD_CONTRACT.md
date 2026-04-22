# Run Record Contract

This document defines the persisted planning contract for `slurmforge`.

The system persists three planning-level objects:

- `RunPlan`: executable per-run plan consumed by array dispatch and the run executor.
- `RunSnapshot`: per-run replay snapshot consumed by replay / retry / rerun flows.
- `batch_manifest.json`: batch-level materialization manifest, including array groups and GPU budget plan.

These objects are related, but they are not interchangeable. Runtime status,
attempt results, checkpoints, train outputs, logs, and artifacts are execution
data, not planning contract.

## Storage Contract

All planning and execution I/O goes through `src/slurmforge/storage/`.

- `PlanningStore` writes and reads planning data.
- `ExecutionStore` writes runtime data and reconciles it into SQLite when needed.

Supported planning backends:

| Engine | Canonical planning source | Recovery files |
| --- | --- | --- |
| `none` | Filesystem JSON | Always present |
| `sqlite` + `storage.exports.planning_recovery: true` | SQLite DB | Also exported |
| `sqlite` + `storage.exports.planning_recovery: false` | SQLite DB | Not written |

`storage.exports.planning_recovery` controls whether planning recovery files are
exported next to the batch. It is not an execution toggle.

When `planning_recovery=false`, these files are intentionally absent:

- `records/group_xx/task_*.json`
- `meta/runs_manifest.jsonl`
- `runs/run_xxx_<run_id>/resolved_config.yaml`
- `runs/run_xxx_<run_id>/meta/run_snapshot.json`
- per-job recovery copies such as `execution_plan.json`

The batch still materializes array sbatch scripts, submit scripts, and
`batch_manifest.json`. SQLite read paths are DB-first. With
`planning_recovery=false`, the SQLite store does not fall back to filesystem
planning records.

Runtime writes always produce files from compute nodes for NFS safety. SQLite
execution tables are populated by reconciliation.

## Planning Persistence

### Filesystem Engine

Filesystem planning data is written under the batch root:

- `batch_manifest.json`
- `meta/runs_manifest.jsonl`
- `records/group_xx/task_000000.json`
- `runs/run_xxx_<run_id>/resolved_config.yaml`
- `runs/run_xxx_<run_id>/meta/run_snapshot.json`

`meta/runs_manifest.jsonl` stores one serialized `RunPlan` per line and is the
fast batch-load path. `records/group_xx/task_*.json` stores the same `RunPlan`
payloads in per-array-task form.

### SQLite Engine

SQLite planning data is written to the configured batch DB. With the default
`sqlite.path: auto`, the DB path is:

```text
<batch_root>/meta/slurmforge.sqlite3
```

Current planning tables:

- `meta`: batch identity and SQLite schema version.
- `array_groups`: array group metadata and rendered group payload.
- `runs`: one serialized `RunPlan` per run in `payload_json`.
- `run_snapshots`: one serialized `RunSnapshot` per run in `payload_json`.
- `planning_diagnostics`: batch planning diagnostics.

The executor and replay/retry flows read through `PlanningStore`; callers should
not guess whether the batch is filesystem-backed or SQLite-backed.

## Executor Locator

The run executor is addressed by logical array coordinates:

```bash
sforge-run-plan-executor \
  --batch-root <batch_root> \
  --group-index <group_index> \
  --task-index <task_index>
```

`load_plan(batch_root, group_index, task_index)` resolves the `RunPlan` through
`create_planning_store_for_read(batch_root)`. The storage layer binds relative
paths in the persisted record back to the selected batch root.

Executors do not read `RunSnapshot`; replay/retry flows do.

## RunPlan Contract

`RunPlan` is the execution contract. It is serialized by
`serialize_run_plan()` in `src/slurmforge/pipeline/records/codecs/run_plan.py`.

Top-level persisted fields:

- `run_index`
- `total_runs`
- `run_id`
- `generated_by`
- `project`
- `experiment_name`
- `model_name`
- `train_mode`
- `train_stage`
- `eval_stage`
- `eval_train_outputs`
- `cluster`
- `env`
- `run_dir_rel`
- `dispatch`
- `artifacts`
- `sweep_case_name`
- `sweep_assignments`
- `planning_diagnostics`

### Execution Fields

`train_stage` is required.

`eval_stage` is either `null` or another `StageExecutionPlan`. Eval execution is
controlled structurally by `eval_stage` and `eval_train_outputs`, not by parsing
shell strings.

`run_dir_rel` is required on persisted records. The absolute `run_dir` value is
rebuilt from the current batch root when records are loaded.

### StageExecutionPlan

Persisted fields:

- `name`
- `stage_kind`
- `invocation_kind`
- `launcher_kind`
- `command_text`
- `workdir`
- `topology`
- `allocation`
- `estimate`
- `capabilities`
- `python_bin`
- `launcher_cfg`
- `cluster_cfg`
- `script_path`
- `cli_args`
- `command_mode`
- `requested_launcher_mode`
- `max_gpus_per_job`
- `diagnostics`

`max_gpus_per_job` is run-scoped. It records the per-run planning cap used when
building the stage. It is independent of the batch-wide
`resources.max_available_gpus`.

### Topology

`topology` is the resolved execution shape:

- `nodes`
- `processes_per_node`
- `master_port`
- `total_processes`

### Allocation

`allocation` is the resolved Slurm resource request for the stage:

- `nodes`
- `gpus_per_node`
- `cpus_per_task`
- `mem`
- `total_gpus`

`total_gpus = nodes * gpus_per_node`.

### Estimate

`estimate` is the GPU recommendation envelope used by planning:

- `min_total_gpus`
- `recommended_total_gpus`
- `max_useful_total_gpus`
- `estimated_vram_gb`
- `reason`

### Capabilities

`capabilities` describes what the stage may do:

- `ddp_supported`
- `ddp_required`
- `uses_gpu`
- `external_launcher`
- `runtime_probe`

### eval_train_outputs

`eval_train_outputs` is the train-to-eval checkpoint handoff policy.

Persisted fields:

- `required`
- `checkpoint_policy`
- `explicit_checkpoint`

### dispatch

`dispatch` stores materialization metadata for array execution:

- `sbatch_path_rel`
- `record_path_rel`
- `array_group`
- `array_task_index`
- `array_assignment`

If `array_assignment` is present, it contains:

- `group_index`
- `group_signature`
- `grouping_fields`
- `group_reason`

In filesystem recovery mode, `record_path_rel` points at
`records/group_xx/task_*.json`. In pure SQLite planning mode
(`planning_recovery=false`), no task record file is created and `record_path_rel`
may be `null`.

### planning_diagnostics

`planning_diagnostics` stores structured diagnostics attached to the plan.

Each diagnostic contains:

- `severity`
- `category`
- `code`
- `message`
- `stage`
- `field_path`
- `actual`
- `expected`
- `hint`

Batch-level diagnostics can also be stored in SQLite `planning_diagnostics` and
reported through compile/materialization reports.

## RunSnapshot Contract

`RunSnapshot` is the replay/retry contract. It is serialized by
`serialize_run_snapshot()` in
`src/slurmforge/pipeline/records/codecs/run_snapshot.py`.

Top-level persisted fields:

- `run_index`
- `total_runs`
- `run_id`
- `generated_by`
- `project`
- `experiment_name`
- `model_name`
- `train_mode`
- `sweep_case_name`
- `sweep_assignments`
- `replay_spec`

`RunSnapshot` is not executed directly. Replay, retry, and rerun use it to
rebuild a new `ExperimentSpec`, then produce new `RunPlan` objects.

## replay_spec Contract

`replay_spec` is the authoritative replay input.

Persisted fields:

- `schema_version`
- `replay_cfg`
- `planning_root`
- `source_batch_root`
- `source_run_id`
- `source_record_path`

Current replay schema version is defined by `CURRENT_REPLAY_SCHEMA_VERSION` in
`src/slurmforge/pipeline/records/replay_spec/model.py`.

### replay_cfg

`replay_cfg` is a complete, sweep-collapsed experiment config for one run. It is
the source used by replay/retry/rerun to reconstruct that run.

It persists:

- run-scoped config, such as `run.*`, `launcher.*`, `cluster.*`, `env.*`,
  `artifacts.*`, `eval.*`, `validation.*`, and most `resources.*`.
- batch-scoped config required to rebuild a coherent batch, such as `project`,
  `experiment_name`, `output.*`, `notify.*`, `storage.*`,
  `resources.max_available_gpus`, and `dispatch.group_overflow_policy`.
- `resolved_model_catalog`, not `model_registry`, so replay does not depend on
  the original authoring-time registry source.

`model_registry.*` is authoring-only. `resolved_model_catalog.*` is replay-only.

### replay provenance

`planning_root` is the path-resolution root used when the snapshot was created.

`source_batch_root`, `source_run_id`, and `source_record_path` describe where the
replay input came from. They are provenance fields, not execution directives.

## Batch-Scoped vs Run-Scoped Config

The run record contract must preserve this boundary:

Batch-scoped fields:

- `project`
- `experiment_name`
- `output.*`
- `notify.*`
- `storage.*`
- `resources.max_available_gpus`
- `dispatch.group_overflow_policy`

Run-scoped fields:

- `model.*`
- `run.*`
- `launcher.*`
- `cluster.*`
- `env.*`
- `artifacts.*`
- `eval.*`
- `validation.*`
- `resources.max_gpus_per_job`
- other per-run `resources.*` knobs such as estimator settings

Replay/rerun behavior:

- `resources.max_available_gpus` candidates are collected from selected runs and
  must resolve to one value.
- `dispatch.group_overflow_policy` candidates are collected from selected runs
  and must resolve to one value.
- CLI `--set` overrides are applied before candidate collection, so overriding a
  batch-scoped field makes the selected candidates agree.
- `resources.max_gpus_per_job` remains run-scoped and may differ per run.

## batch_manifest.json Contract

`batch_manifest.json` is the batch-level materialization contract.

Persisted fields:

- `generated_by`
- `project`
- `experiment_name`
- `batch_name`
- `dispatch_mode`
- `total_runs`
- `array_group_count`
- `batch_root`
- `sbatch_dir`
- `submit_script`
- `resource_buckets`
- `array_groups`
- `runs_manifest`
- `notify`
- `submit_dependencies`
- `gpu_budget_plan`

Replay/retry source metadata may add extra top-level sections, such as
`replay_source` or `retry_source`.

`runs_manifest` is the conventional path for filesystem recovery. In pure
SQLite planning mode, this path may point to a file that was intentionally not
written.

## gpu_budget_plan Contract

`gpu_budget_plan` is the batch-level GPU scheduling plan consumed by validate,
dry-run, and materialization.

Top-level fields:

- `max_available_gpus`
- `group_overflow_policy`
- `policy_applied`
- `min_concurrent_gpus`
- `max_planned_concurrent_gpus`
- `strict_global_limit`
- `groups`
- `warnings`

`policy_applied` is one of:

- `shared_budget`
- `serialized_groups`
- `best_effort`

There is no persisted `error` policy result. If `group_overflow_policy=error`
cannot produce a valid plan, planning fails and no `gpu_budget_plan` is emitted.

### GPU Budget Group

Each entry in `groups` contains:

- `group_id`
- `task_count`
- `gpus_per_task`
- `throttle`
- `max_group_gpus`
- `limiting_run`
- `limiting_model`
- `max_estimated_gpus`
- `constrained`
- `constraint_reason`

Definitions:

- `gpus_per_task = train_stage.allocation.nodes * train_stage.allocation.gpus_per_node`
- `max_group_gpus = throttle * gpus_per_task`
- `min_concurrent_gpus = sum(gpus_per_task for all groups)`
- `strict_global_limit = max_planned_concurrent_gpus <= max_available_gpus`

For `shared_budget`, `max_planned_concurrent_gpus` is the sum of all
`max_group_gpus` and must be within `max_available_gpus`.

For `serialized_groups`, only one group is intended to run at a time, so
`max_planned_concurrent_gpus` is the maximum `max_group_gpus` across groups.

For `best_effort`, each group is throttled independently, but the sum across
groups may exceed `max_available_gpus`; this is why `strict_global_limit` may be
false and a warning is persisted.

CPU-only dispatch is not supported by this project. A group with
`gpus_per_task <= 0` is a planning error.

## Non-Contract Runtime Files

The following files are runtime artifacts or recovery conveniences. They are not
the planning contract:

- job stdout / stderr
- train / eval logs
- `job-<job_key>/meta/execution_status.json`
- `job-<job_key>/meta/attempt_result.json`
- `job-<job_key>/meta/checkpoint_state.json`
- `job-<job_key>/meta/train_outputs.json`
- `job-<job_key>/meta/train_outputs.env`
- `job-<job_key>/meta/artifact_manifest.json`
- `job-<job_key>/meta/execution_plan.json`
- `job-<job_key>/meta/resolved_config.yaml`
- `job-<job_key>/meta/run_snapshot.json`

These files may be used for recovery, debugging, or execution reconciliation,
but replay/retry planning should go through `PlanningStore` and
`RunSnapshot.replay_spec`.

## Design Rules

- Execution-critical data is stored structurally, not inferred from log text or
  shell command strings.
- `RunPlan` is for execution; `RunSnapshot` is for replay/retry.
- Batch-scoped config is resolved once per batch; run-scoped config stays on each
  run.
- GPU budget planning is batch-level and lives in `gpu_budget_plan`, not in
  per-run stage fields.
- Runtime writes are file-based for NFS safety, even when SQLite is enabled.
- Callers should use `PlanningStore` / `ExecutionStore`, not direct paths, unless
  they are implementing a storage backend.
