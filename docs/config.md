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

runs:
  type: "grid"
  axes:
    train.entry.args.lr: [1e-4, 5e-5]

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

`runs` controls how many run instances are planned. Omit it for one run, use `type: grid` with `axes` for cartesian sweeps, or use `type: cases` with named cases for hand-authored variants.

`runs.axes` and `runs.cases[].set` may address stage paths directly, such as `train.entry.args.lr`; this is normalized to `stages.train.entry.args.lr`.

## Runtime

`runtime.executor.python.bin` is the Python used to run `python -m slurmforge.executor.stage`.

`runtime.user.<name>.python.bin` is the Python used for user stage code. Both contracts are recorded in plans, checked by `--dry-run=full`, and written to `runtime_probe.json` during execution.

## Resources And Sizing

`hardware.gpu_types` defines named GPU profiles for sizing and scheduler defaults. If `resources.gpu_type` references a profile with `slurm.constraint`, the planner uses it unless `resources.constraint` explicitly overrides it.

If `resources.gpus_per_node` is numeric, the stage uses that fixed GPU count and must not declare `gpu_sizing`. If it is `"auto"`, `gpu_sizing` is required and the planner resolves a concrete GPU count from target memory, usable memory, safety factor, rounding, and min/max bounds.

## Inputs And Outputs

Stage inputs are resolved by the contract kernel, not by CLI-specific code paths. `sforge run` resolves `source.kind=upstream_output` inputs from successful upstream outputs. `sforge eval --checkpoint`, `--from-run`, and `--from-train-batch` are source shortcuts that bind the selected eval input.

`checkpoint` is a conventional input/output name, not a schema requirement. A config may use another input name when it declares the source and injection contract explicitly.

`artifact_store.strategy` controls how produced checkpoints and declared artifacts are managed. Supported strategies are `copy`, `hardlink`, `symlink`, and `register_only`.
