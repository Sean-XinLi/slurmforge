# Config Reference

Configs are stage-driven. Stage-batch v1 supports `train` and `eval`; `eval` may depend on `train`.

Recommended top-level order:

```text
project, experiment, storage, hardware, environments, runtime, sizing,
artifact_store, notifications, runs, stages, dispatch, orchestration
```

## Shape

```yaml
project: "demo_project"
experiment: "finetune_v2"

storage:
  root: "./runs"

hardware:
  gpu_types:
    a100_80gb:
      memory_gb: 80
      usable_memory_fraction: 0.90
      max_gpus_per_node: 8
      slurm:
        constraint: "a100"

environments:
  cluster_env:
    modules:
      - "cuda/12.1"
    source:
      - path: "/shared/miniconda/bin/activate"
        args: ["myenv"]
    env:
      HF_HOME: "/shared/hf"

runtime:
  executor:
    python:
      bin: "python3.11"
      min_version: "3.10"
    module: "slurmforge.executor.stage"
  user:
    default:
      python:
        bin: "python3.11"
        min_version: "3.10"

sizing:
  gpu:
    defaults:
      safety_factor: 1.15
      round_to: 1

artifact_store:
  strategy: "copy"
  fallback_strategy: null
  verify_digest: true
  fail_on_verify_error: true

notifications:
  email:
    enabled: false
    to: []
    "on":
      - "batch_finished"
      - "train_eval_pipeline_finished"
    mode: "summary"
    from: "slurmforge@localhost"
    sendmail: "/usr/sbin/sendmail"
    subject_prefix: "SlurmForge"

runs:
  type: "matrix"
  cases:
    - name: "small_model"
      set:
        train.entry.args.model: "small"
        train.entry.args.batch_size: 16
      axes:
        train.entry.args.lr: [1e-4, 5e-5]
        train.entry.args.seed: [1, 2]
    - name: "large_model"
      set:
        train.entry.args.model: "large"
        train.entry.args.batch_size: 8
      axes:
        train.entry.args.lr: [5e-5, 1e-5]
        train.entry.args.seed: [1, 2]

stages:
  train:
    enabled: true
    environment: cluster_env
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
      nproc_per_node: auto
      rendezvous:
        backend: c10d
        endpoint: auto
        port: 29500
    resources:
      partition: "gpu"
      nodes: 1
      gpu_type: "a100_80gb"
      gpus_per_node: "auto"
      cpus_per_task: 12
      mem: "256G"
    gpu_sizing:
      estimator: "heuristic"
      target_memory_gb: 192
      min_gpus_per_job: 4
      max_gpus_per_job: 4
    outputs:
      checkpoint:
        kind: file
        required: true
        discover:
          globs: ["checkpoints/**/*.pt"]
          select: "latest_step"

  eval:
    enabled: true
    environment: cluster_env
    depends_on: ["train"]
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
          mode: "path"
    resources:
      partition: "gpu"
      nodes: 1
      gpu_type: "a100_80gb"
      gpus_per_node: 1
      cpus_per_task: 8
      mem: "32G"
    outputs:
      accuracy:
        kind: metric
        file: "eval/metrics.json"
        json_path: "$.accuracy"
        required: true

dispatch:
  max_available_gpus: 32
  overflow_policy: "serialize_groups"

orchestration:
  controller:
    partition: "cpu"
    cpus: 1
    mem: "2G"
    time_limit: "01:00:00"
    environment: "cluster_env"
```

## Runs

`runs` controls how many run instances are planned. Omit it for one run, use `type: cases` for named hand-authored scenarios, `type: grid` for cartesian sweeps, or `type: matrix` for named scenarios that each contain their own grid.

Use `cases` when each run needs a meaningful name or a custom combination of settings:

```yaml
runs:
  type: "cases"
  cases:
    - name: "small_lr"
      set:
        train.entry.args.lr: 1e-4
        train.entry.args.epochs: 3
    - name: "low_lr_longer"
      set:
        train.entry.args.lr: 5e-5
        train.entry.args.epochs: 5
        eval.entry.args.split: "validation"
```

Use `grid` when you want every combination of axis values:

```yaml
runs:
  type: "grid"
  axes:
    train.entry.args.lr: [1e-4, 5e-5]
    train.entry.args.seed: [1, 2]
```

Use `matrix` when you need named scenarios and a grid inside each scenario:

```yaml
runs:
  type: "matrix"
  cases:
    - name: "small_model"
      set:
        train.entry.args.model: "small"
        train.entry.args.batch_size: 16
      axes:
        train.entry.args.lr: [1e-4, 5e-5]
        train.entry.args.seed: [1, 2]
    - name: "large_model"
      set:
        train.entry.args.model: "large"
        train.entry.args.batch_size: 8
      axes:
        train.entry.args.lr: [5e-5, 1e-5]
        train.entry.args.seed: [1, 2]
```

For `matrix`, each final run is `base config + case.set + one case.axes grid point`. A path may appear in `case.set` or in `case.axes`, but not both.

`runs.axes`, `runs.cases[].set`, and `runs.cases[].axes` may address stage paths directly, such as `train.entry.args.lr`; this is normalized to `stages.train.entry.args.lr`.

CLI overrides use the same path shorthand. These two commands target the same field:

```bash
sforge validate --config experiment.yaml --set train.entry.args.lr=0.001
sforge validate --config experiment.yaml --set stages.train.entry.args.lr=0.001
```

## Field Options

This table is generated from `slurmforge.field_options.FieldCatalog`.

| Field | Options | Meaning |
| --- | --- | --- |
| `artifact_store.fallback_strategy` | `null`, `copy`, `hardlink`, `symlink`, `register_only` | `null`: Disable fallback handling.<br>`copy`: Copy artifacts when the primary strategy fails.<br>`hardlink`: Hardlink artifacts when supported.<br>`symlink`: Symlink artifacts when supported.<br>`register_only`: Record artifacts without copying files. |
| `artifact_store.strategy` | `copy`, `hardlink`, `symlink`, `register_only` | `copy`: Copy managed artifacts into the run store.<br>`hardlink`: Hardlink managed artifacts into the run store.<br>`symlink`: Symlink managed artifacts into the run store.<br>`register_only`: Track artifact paths without copying files. |
| `dispatch.overflow_policy` | `serialize_groups`, `error`, `best_effort` | `serialize_groups`: Queue array groups within GPU budget.<br>`error`: Reject plans that exceed the GPU budget.<br>`best_effort`: Submit groups without strict serialization. |
| `notifications.email.mode` | `summary` | `summary`: Send a compact workflow summary. |
| `notifications.email.on` | `batch_finished`, `train_eval_pipeline_finished` | `batch_finished`: Send after a stage batch reaches terminal state.<br>`train_eval_pipeline_finished`: Send after a train/eval pipeline reaches terminal state. |
| `runs.type` | `single`, `grid`, `cases`, `matrix` | `single`: Plan one run.<br>`grid`: Plan every combination from top-level axes.<br>`cases`: Plan named hand-authored run variants.<br>`matrix`: Plan named cases, each with its own grid. |
| `stages.*.entry.type` | `python_script`, `command` | `python_script`: Run a Python file.<br>`command`: Run a shell command. |
| `stages.*.inputs.*.expects` | `path`, `manifest`, `value` | `path`: Inject a filesystem path.<br>`manifest`: Inject a manifest payload.<br>`value`: Inject a scalar value. |
| `stages.*.inputs.*.inject.mode` | `path`, `value`, `json` | `path`: Pass the resolved input path.<br>`value`: Pass the resolved input value.<br>`json`: Pass the resolved input encoded as JSON. |
| `stages.*.inputs.*.source.kind` | `upstream_output`, `external_path` | `upstream_output`: Read an output from a previous stage.<br>`external_path`: Read an explicit user-provided path. |
| `stages.*.launcher.mode` | `single_node`, `multi_node` | `single_node`: Launch on one node.<br>`multi_node`: Launch across multiple nodes. |
| `stages.*.launcher.type` | `single`, `python`, `torchrun`, `srun`, `mpirun`, `command` | `single`: Run one process directly.<br>`python`: Launch through Python.<br>`torchrun`: Launch distributed PyTorch.<br>`srun`: Launch through Slurm srun.<br>`mpirun`: Launch through MPI.<br>`command`: Launch a raw command. |
| `stages.*.outputs.*.discover.select` | `latest_step`, `first`, `last` | `latest_step`: Pick the path with the highest step number.<br>`first`: Pick the first sorted match.<br>`last`: Pick the last sorted match. |
| `stages.*.outputs.*.kind` | `file`, `files`, `metric`, `manifest` | `file`: One managed file.<br>`files`: Multiple discovered files.<br>`metric`: A metric value read from JSON.<br>`manifest`: A manifest JSON file. |

## Runtime

`runtime.executor.python.bin` is the Python used to run `python -m slurmforge.executor.stage`.

`runtime.user.<name>.python.bin` is the Python used for user stage code. Both contracts are recorded in plans, checked by `--dry-run=full`, and written to `runtime_probe.json` during execution.

## Notifications

Notifications are disabled unless `notifications.email.enabled` is true. When enabled, `notifications.email.to` must contain at least one email address and `notifications.email.on` must contain one or more supported terminal workflow events.

`batch_finished` is emitted by stage-batch finalizer jobs. `train_eval_pipeline_finished` is emitted by the train/eval controller when the whole pipeline reaches a terminal state.

## Resources And Sizing

`hardware.gpu_types` defines named GPU profiles for sizing and scheduler defaults. If `resources.gpu_type` references a profile with `slurm.constraint`, the planner uses it unless `resources.constraint` explicitly overrides it.

If `resources.gpus_per_node` is numeric, the stage uses that fixed GPU count and must not declare `gpu_sizing`. If it is `"auto"`, `gpu_sizing` is required and the planner resolves a concrete GPU count from target memory, usable memory, safety factor, rounding, and min/max bounds.

## Inputs And Outputs

Stage inputs are resolved by the contract kernel, not by CLI-specific code paths. `sforge run` resolves `source.kind=upstream_output` inputs from successful upstream outputs. `sforge eval --checkpoint`, `--from-run`, and `--from-train-batch` are source shortcuts that bind the selected eval input.

`checkpoint` is a conventional input/output name, not a schema requirement. A config may use another input name when it declares the source and injection contract explicitly.

`artifact_store.strategy` controls how produced checkpoints and declared artifacts are managed. Supported strategies are listed in Field Options.
