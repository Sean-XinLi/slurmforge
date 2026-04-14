# Run Record Contract

`slurmforge` persists two different planning objects:

- `RunPlan`: the executable plan consumed by generators, dispatch, and executors
- `RunSnapshot`: the replay snapshot consumed by retry / replay flows

They are related, but they are not interchangeable.

## Storage Layer

All planning and execution I/O goes through the unified storage layer in
[`src/slurmforge/storage/`](src/slurmforge/storage/):

- **`PlanningStore`** — writes and reads planning artefacts (`RunPlan`, `RunSnapshot`, manifests)
- **`ExecutionStore`** — writes runtime artefacts (execution status, attempt results, checkpoints)

Two backends exist:

| Engine | Config | Source of truth |
|--------|--------|-----------------|
| `none` (default) | `storage.backend.engine: none` | Filesystem JSON files |
| `sqlite` | `storage.backend.engine: sqlite` | Files + per-batch SQLite at `meta/slurmforge.sqlite3` |

The SQLite backend mirrors all planning data into indexed DB tables. Runtime writes
(from compute nodes over NFS) always produce files regardless of engine; the DB is
reconciled lazily via `ExecutionStore.reconcile_batch()`.

### `planning_exports`

`storage.exports.planning_exports: false` (default) suppresses copying the run
record JSON (`execution_plan.json`) into each job result directory during finalization.
The canonical record locations (`records/group_xx/task_*.json`, `meta/runs_manifest.jsonl`)
remain the source of truth and are unaffected by this setting.

## Persistence Boundaries

### `RunPlan`

`RunPlan` is serialized by `serialize_run_plan()` and deserialized by `deserialize_run_plan()` in
[`src/slurmforge/pipeline/records/codecs/run_plan.py`](src/slurmforge/pipeline/records/codecs/run_plan.py).

Batch-level loading goes through `PlanningStore.load_batch_run_plans()`, which reads:

- `meta/runs_manifest.jsonl` (fast path, written during materialization)
- `records/group_xx/task_*.json` (fallback for legacy batches without a manifest)

Equivalent persisted forms (filesystem engine):

- `records/group_xx/task_000000.json`
- `meta/runs_manifest.jsonl`

In the SQLite engine, the same payloads are also stored as `payload_json` in the `runs` table.

### `RunSnapshot`

`RunSnapshot` is serialized by `serialize_run_snapshot()` and deserialized by `deserialize_run_snapshot()` in
[`src/slurmforge/pipeline/records/codecs/run_snapshot.py`](src/slurmforge/pipeline/records/codecs/run_snapshot.py).

Run-directory loading uses `PlanningStore.load_run_snapshot()`.

Persisted form (filesystem):

- `runs/run_xxx_<hash>/meta/run_snapshot.json`

In the SQLite engine, also mirrored in the `run_snapshots` table.

## Executor Locator

The executor (`sforge-run-plan-executor`) is invoked with logical locators instead of a
file path:

```
sforge-run-plan-executor \
  --batch-root  <batch_root>  \
  --group-index <group_index> \
  --task-index  <task_index>
```

`load_plan(batch_root, group_index, task_index)` resolves the `RunPlan` via
`create_planning_store_for_read(batch_root)`, which auto-detects the engine from
the on-disk layout (presence of `meta/slurmforge.sqlite3`).

This replaces the legacy `--record PATH` interface.

## Non-Contract Runtime Files

These files are runtime artefacts, not planning contract:

- `runs/run_xxx_<hash>/resolved_config.yaml`
- `job-<job_key>/meta/execution_status.json`
- `job-<job_key>/meta/attempt_result.json`
- `job-<job_key>/meta/checkpoint_state.json`
- `job-<job_key>/meta/train_outputs.json`
- `job-<job_key>/meta/train_outputs.env`
- `job-<job_key>/execution_plan.json` (only written when `planning_exports: true`)
- train / eval logs
- Slurm stdout / stderr

## Design Rules

- execution-critical data is stored structurally, not inferred from free-form strings
- train / eval execution uses `StageExecutionPlan`
- train→eval checkpoint handoff uses `eval_train_outputs`, not ad hoc shell flags
- replay rebuilds from `RunSnapshot.replay_spec`
- diagnostics are persisted as structured records
- runtime writes always produce files regardless of storage engine (NFS safety)

## `RunPlan` Contract

### Top-level fields

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

### `train_stage` / `eval_stage`

`train_stage` is required. `eval_stage` is either `null` or a second `StageExecutionPlan`.

Persisted `StageExecutionPlan` fields:

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
- `max_available_gpus_per_node`
- `diagnostics`

### `eval_train_outputs`

`eval_train_outputs` is the persisted train→eval checkpoint handoff policy.

Persisted fields:

- `required`
- `checkpoint_policy`
- `explicit_checkpoint`

### `topology`

`topology` is the resolved execution shape:

- `nodes`
- `processes_per_node`
- `master_port`
- `total_processes`

### `allocation`

`allocation` is the resolved Slurm request for that stage:

- `nodes`
- `gpus_per_node`
- `cpus_per_task`
- `mem`
- `total_gpus`

### `estimate`

`estimate` is the planner-facing GPU recommendation envelope:

- `min_total_gpus`
- `recommended_total_gpus`
- `max_useful_total_gpus`
- `estimated_vram_gb`
- `reason`

### `capabilities`

`capabilities` expresses what the stage is allowed to do:

- `ddp_supported`
- `ddp_required`
- `uses_gpu`
- `external_launcher`
- `runtime_probe`

### `planning_diagnostics`

`planning_diagnostics` is a batch-level list of structured validator output.

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

### `dispatch`

`dispatch` stores batch-relative materialization metadata:

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

## `RunSnapshot` Contract

### Top-level fields

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

### `replay_spec`

`replay_spec` is the authoritative retry / replay input.

Persisted fields:

- `schema_version`
- `replay_cfg`
- `planning_root`
- `source_batch_root`
- `source_run_id`
- `source_record_path`

Semantics:

- `replay_cfg` is already sweep-collapsed to one run
- `planning_root` is the original path-resolution root
- provenance fields describe where the run came from, not how to execute it

## Executor Minimum Surface

If you only care about what an executor needs, the minimum useful surface is:

- `train_stage`
- `eval_stage`
- `eval_train_outputs`
- `run_dir_rel`
- `cluster`
- `env`
- `artifacts`
