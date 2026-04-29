# Config Reference

Configs are stage-driven. Stage-batch v1 supports `train` and `eval`; `eval` may depend on `train`.

Recommended top-level order:

```text
project, experiment, storage, hardware, environments, runtime, sizing,
artifact_store, notifications, runs, stages, dispatch, orchestration
```

## Starter Shape

<!-- CONFIG_STARTER_EXAMPLE_START -->
```yaml
# Starter template: train-eval
# Edit the values below, then run `sforge validate --config <file>`.
# Full field reference: docs/config.md

# Names the project namespace used in generated storage paths.
project: demo
# Names this experiment inside the project namespace.
experiment: baseline

storage:
  # Root directory for plans, logs, status records, and managed artifacts.
  root: ./runs

environments:
  default:
    # Module names loaded before executor and user scripts run.
    modules: []
    # Shell files or setup commands sourced before execution.
    source: []
    # Static environment variables added to Slurm job environments.
    env: {}

runtime:
  executor:
    python:
      # Python used by the SlurmForge executor wrapper on compute nodes.
      bin: python3
      min_version: "3.10"  # Minimum Python version required for the SlurmForge executor.
    # Python module used as the SlurmForge stage executor entrypoint.
    module: slurmforge.executor.stage
  user:
    default:
      python:
        # Python used to run generated train/eval scripts.
        bin: python3
        min_version: "3.10"  # Minimum Python version required for user stage scripts.
      # Environment variables visible to user stage scripts.
      env: {}

artifact_store:
  # Options: copy, hardlink, symlink, register_only. Controls how declared outputs are placed in the managed run store.
  strategy: copy
  # Options: null, copy, hardlink, symlink, register_only. Fallback behavior when the primary artifact storage strategy fails.
  fallback_strategy: null
  # Verifies managed output digests after artifact storage.
  verify_digest: true
  # Fails the run when artifact verification cannot prove integrity.
  fail_on_verify_error: true

notifications:
  email:
    # Enables email summary notifications for terminal workflow events.
    enabled: false
    # Recipients for email notifications when email is enabled.
    to: []
    # Options: batch_finished, train_eval_pipeline_finished. Terminal workflow events that trigger email summaries.
    "on":
      - batch_finished
    # Options: summary. Controls email content shape.
    mode: summary
    # Sender address used by email delivery.
    from: "slurmforge@localhost"
    # Local sendmail-compatible binary used to deliver email.
    sendmail: /usr/sbin/sendmail
    # Prefix added to SlurmForge notification email subjects.
    subject_prefix: SlurmForge

runs:
  # Options: single, grid, cases, matrix. Controls whether the config plans one run or expands a sweep.
  type: single

dispatch:
  # GPU budget used to serialize Slurm array groups when a plan exceeds available GPUs.
  max_available_gpus: 1
  # Options: serialize_groups, error, best_effort. Controls planner behavior when run groups exceed the declared GPU budget.
  overflow_policy: serialize_groups

orchestration:
  controller:
    # Slurm partition for the lightweight train/eval controller job.
    partition: gpu
    # CPU count requested by the lightweight workflow controller job.
    cpus: 1
    # Memory requested by the lightweight workflow controller job.
    mem: 2G
    # Slurm time limit requested by the lightweight workflow controller job.
    time_limit: "01:00:00"
    # Environment profile loaded before the lightweight workflow controller runs.
    environment: default

stages:
  train:
    # Stage role used by starter workflows and command selection.
    kind: train
    # Controls whether the stage participates in planning.
    enabled: true
    # Environment profile used before the stage runs.
    environment: default
    # User runtime profile used to execute the stage script.
    runtime: default
    entry:
      # Options: python_script, command. Controls how the stage command is interpreted.
      type: python_script
      # Script path resolved relative to the config file directory.
      script: train.py
      # Working directory used when launching the stage command.
      workdir: .
      # Mapping passed to each stage script as CLI flags.
      args:
        epochs: 1
        lr: 0.001
    launcher:
      # Options: single, python, torchrun, srun, mpirun, command. Chooses how a stage process is launched.
      type: single
    resources:
      # Slurm partition or queue for each stage task.
      partition: gpu
      # Number of nodes requested by each stage task.
      nodes: 1
      # GPU count per node, or auto when GPU sizing is configured.
      gpus_per_node: 1
      # CPU count requested for each stage task.
      cpus_per_task: 4
      # Memory requested for each stage task.
      mem: 16G
      # Slurm time limit requested for each stage task.
      time_limit: "01:00:00"
    outputs:
      checkpoint:
        # Options: file, files, metric, manifest. Declares the shape of a managed stage output.
        kind: file
        required: true
        discover:
          # Glob patterns evaluated under the stage run directory.
          globs:
            - checkpoints/**/*.pt
          # Options: latest_step, first, last. Selects one path from discovered output glob matches.
          select: latest_step

  eval:
    # Stage role used by starter workflows and command selection.
    kind: eval
    # Controls whether the stage participates in planning.
    enabled: true
    # Environment profile used before the stage runs.
    environment: default
    # User runtime profile used to execute the stage script.
    runtime: default
    entry:
      # Options: python_script, command. Controls how the stage command is interpreted.
      type: python_script
      # Script path resolved relative to the config file directory.
      script: eval.py
      # Working directory used when launching the stage command.
      workdir: .
      # Mapping passed to each stage script as CLI flags.
      args:
        split: validation
    launcher:
      # Options: single, python, torchrun, srun, mpirun, command. Chooses how a stage process is launched.
      type: single
    resources:
      # Slurm partition or queue for each stage task.
      partition: gpu
      # Number of nodes requested by each stage task.
      nodes: 1
      # GPU count per node, or auto when GPU sizing is configured.
      gpus_per_node: 1
      # CPU count requested for each stage task.
      cpus_per_task: 2
      # Memory requested for each stage task.
      mem: 8G
      # Slurm time limit requested for each stage task.
      time_limit: "01:00:00"
    outputs:
      accuracy:
        # Options: file, files, metric, manifest. Declares the shape of a managed stage output.
        kind: metric
        required: true
        # JSON file produced by the stage and read by output discovery.
        file: eval/metrics.json
        # JSONPath used to read a metric value from the output file.
        json_path: $.accuracy
    # Upstream stage names that must complete before this stage is selected.
    depends_on:
      - train
    inputs:
      checkpoint:
        source:
          # Options: upstream_output, external_path. Controls where a stage input is resolved from.
          kind: upstream_output
          # Producer stage name for an upstream output input.
          stage: train
          # Producer output name resolved from the upstream stage.
          output: checkpoint
        # Options: path, manifest, value. Declares the resolved input shape expected by the consuming stage.
        expects: path
        required: true
        inject:
          # CLI flag name used to pass the resolved input to the stage process.
          flag: checkpoint_path
          # Environment variable used to pass the resolved input to the stage process.
          env: SFORGE_INPUT_CHECKPOINT
          # Options: path, value, json. Controls how a resolved input is injected into the stage process.
          mode: path
```
<!-- CONFIG_STARTER_EXAMPLE_END -->

## Advanced Shape

<!-- CONFIG_ADVANCED_EXAMPLE_START -->
```yaml
project: resnet
experiment: ablation_matrix

storage:
  root: /shared/runs

hardware:
  gpu_types:
    a100_80gb:
      memory_gb: 80
      usable_memory_fraction: 0.9
      max_gpus_per_node: 8
      slurm:
        constraint: a100

environments:
  default:
    modules:
      - cuda/12.1
    source:
      - path: /shared/envs/slurmforge.sh
        args:
          - train
    env:
      HF_HOME: /shared/cache/huggingface

runtime:
  executor:
    python:
      bin: python3
      min_version: "3.10"
    module: slurmforge.executor.stage
  user:
    default:
      python:
        bin: /shared/envs/train/bin/python
        min_version: "3.10"
      env:
        TOKENIZERS_PARALLELISM: "false"

sizing:
  gpu:
    defaults:
      safety_factor: 1.15
      round_to: 1

artifact_store:
  strategy: hardlink
  fallback_strategy: copy
  verify_digest: true
  fail_on_verify_error: true

notifications:
  email:
    enabled: true
    to:
      - ml-team@example.com
    "on":
      - train_eval_pipeline_finished
    mode: summary
    from: slurmforge@example.com
    sendmail: /usr/sbin/sendmail
    subject_prefix: SlurmForge

runs:
  type: matrix
  cases:
    - name: small
      set:
        train.entry.args.model: resnet18
      axes:
        train.entry.args.lr: [0.001, 0.0005]
        train.entry.args.seed: [1, 2]
    - name: large
      set:
        train.entry.args.model: resnet50
        train.resources.gpu_type: a100_80gb
      axes:
        train.entry.args.lr: [0.0005]
        train.entry.args.seed: [1, 2]

dispatch:
  max_available_gpus: 16
  overflow_policy: serialize_groups

orchestration:
  controller:
    partition: gpu
    cpus: 1
    mem: 2G
    time_limit: "12:00:00"
    environment: default

stages:
  train:
    kind: train
    enabled: true
    environment: default
    runtime: default
    before:
      - name: prepare_cache
        run: mkdir -p "$HF_HOME"
    entry:
      type: python_script
      script: train.py
      workdir: .
      args:
        epochs: 5
        batch_size: 64
    launcher:
      type: torchrun
      mode: multi_node
      nnodes: auto
      nproc_per_node: auto
      rendezvous:
        backend: c10d
        endpoint: auto
        port: 29500
      srun_args:
        - --cpu-bind=cores
    resources:
      partition: gpu
      account: research
      qos: normal
      time_limit: "06:00:00"
      gpu_type: a100_80gb
      nodes: 2
      gpus_per_node: auto
      cpus_per_task: 16
      mem: 128G
      constraint: a100
      extra_sbatch_args:
        - --exclusive
    gpu_sizing:
      estimator: static_memory
      target_memory_gb: 120
      min_gpus_per_job: 2
      max_gpus_per_job: 16
      safety_factor: 1.2
      round_to: 2
    outputs:
      checkpoint:
        kind: file
        required: true
        discover:
          globs:
            - checkpoints/**/*.pt
          select: latest_step

  eval:
    kind: eval
    enabled: true
    depends_on:
      - train
    environment: default
    runtime: default
    entry:
      type: python_script
      script: eval.py
      workdir: .
      args:
        split: validation
    launcher:
      type: single
    resources:
      partition: gpu
      time_limit: "01:00:00"
      gpu_type: a100_80gb
      nodes: 1
      gpus_per_node: 1
      cpus_per_task: 4
      mem: 32G
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
          mode: path
    outputs:
      accuracy:
        kind: metric
        required: true
        file: eval/metrics.json
        json_path: $.accuracy
```
<!-- CONFIG_ADVANCED_EXAMPLE_END -->

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

## Field Reference

This table is generated from `slurmforge.config_schema.ConfigField`. Starter READMEs intentionally do not duplicate this full reference; `sforge init` writes a template-scoped `CONFIG.sforge.md` instead.

<!-- CONFIG_SCHEMA_REFERENCE_START -->
| Field | Type | Required | Level | Default | Options | Meaning | When To Change |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `experiment` | value | contextual | common | baseline |  | Names this experiment inside the project namespace. | Change this for each baseline, ablation, sweep, or production run family. |
| `project` | value | contextual | common | demo |  | Names the project namespace used in generated storage paths. | Change this before the first real submit so runs land under your project name. |
| `artifact_store.fail_on_verify_error` | value | contextual | advanced | true |  | Fails the run when artifact verification cannot prove integrity. | Disable only when incomplete verification should be recorded as a warning instead of failing the run. |
| `artifact_store.fallback_strategy` | enum | contextual | advanced | null | `null`, `copy`, `hardlink`, `symlink`, `register_only` | Fallback behavior when the primary artifact storage strategy fails. | Use this only when the primary strategy depends on filesystem features that may be unavailable. |
| `artifact_store.strategy` | enum | contextual | advanced | copy | `copy`, `hardlink`, `symlink`, `register_only` | Controls how declared outputs are placed in the managed run store. | Keep copy for the starter; change only after validating filesystem support and retention policy. |
| `artifact_store.verify_digest` | value | contextual | advanced | true |  | Verifies managed output digests after artifact storage. | Keep enabled unless artifact verification is too expensive for the target filesystem. |
| `storage.root` | value | contextual | common | ./runs |  | Root directory for plans, logs, status records, and managed artifacts. | Change this when runs should be written to a shared filesystem or scratch mount. |
| `hardware.gpu_types` | mapping | contextual | advanced | {} |  | Named GPU profiles available to workflow stages. | Define this when GPU sizing or stage placement should reason about accelerator memory and Slurm constraints. |
| `hardware.gpu_types.*.max_gpus_per_node` | integer | contextual | advanced | null |  | Maximum number of GPUs available per node for this GPU type. | Set this when automatic GPU sizing must respect per-node accelerator capacity. |
| `hardware.gpu_types.*.memory_gb` | float | contextual | advanced | 0 |  | Nominal memory in GB for one GPU of this type. | Set this to let GPU sizing estimate the number of GPUs needed for a stage. |
| `hardware.gpu_types.*.slurm.constraint` | value | contextual | advanced | null |  | Slurm constraint associated with this GPU type. | Set this when choosing a gpu_type should imply a cluster-specific node constraint. |
| `hardware.gpu_types.*.usable_memory_fraction` | float | contextual | advanced | 0 |  | Fraction of nominal GPU memory treated as available to jobs. | Lower this when framework overhead, fragmentation, or site policy means full GPU memory is not usable. |
| `environments.*.env` | value | contextual | intermediate | {} |  | Static environment variables added to Slurm job environments. | Add paths, tokens, offline cache settings, or framework environment knobs required by scripts. |
| `environments.*.modules` | value | contextual | intermediate | [] |  | Module names loaded before executor and user scripts run. | Add cluster modules such as CUDA, compiler, MPI, or Python stacks required by your jobs. |
| `environments.*.source` | value | contextual | intermediate | [] |  | Shell files or setup commands sourced before execution. | Use this for virtualenv, conda, or site-specific setup that must run inside Slurm jobs. |
| `environments.*.source[].args` | list | contextual | advanced | [] |  | Arguments passed to the sourced setup file. | Use this when a setup script accepts mode, environment, or module arguments. |
| `environments.*.source[].path` | path | contextual | intermediate | template-specific |  | Shell file sourced before execution. | Use this for conda activation, virtualenv activation, or site setup scripts. |
| `runtime.executor.module` | python-module | contextual | advanced | slurmforge.executor.stage |  | Python module used as the SlurmForge stage executor entrypoint. | Most workflows should keep the packaged executor module. |
| `runtime.executor.python.bin` | value | contextual | common | python3 |  | Python used by the SlurmForge executor wrapper on compute nodes. | Change this when compute nodes need a different Python command to import slurmforge. |
| `runtime.executor.python.min_version` | version | contextual | advanced | 3.10 |  | Minimum Python version required for the SlurmForge executor. | Change only when the executor runtime contract changes. |
| `runtime.user.*.env` | mapping | contextual | intermediate | {} |  | Environment variables visible to user stage scripts. | Use this for variables that should apply to all scripts using the default user runtime. |
| `runtime.user.*.python.bin` | value | contextual | common | python3 |  | Python used to run generated train/eval scripts. | Change this to the Python environment that contains your model dependencies. |
| `runtime.user.*.python.min_version` | version | contextual | advanced | 3.10 |  | Minimum Python version required for user stage scripts. | Change this when your user runtime intentionally targets another Python version. |
| `sizing.gpu.defaults` | mapping | contextual | advanced | safety_factor=1.0, round_to=1 |  | Default safety and rounding policy for automatic GPU sizing. | Define this when stages use gpu_sizing and should share conservative sizing behavior. |
| `sizing.gpu.defaults.round_to` | integer | contextual | advanced | 1 |  | GPU count granularity used after automatic sizing. | Use this to round GPU counts to scheduler or launcher-friendly sizes. |
| `sizing.gpu.defaults.safety_factor` | float | contextual | advanced | 1.0 |  | Multiplier applied to estimated GPU memory before converting to GPU count. | Increase this when estimates are optimistic or workloads have variable peak memory. |
| `runs.axes` | mapping | contextual | intermediate | contextual |  | Top-level grid sweep axes for runs.type=grid. | Use this when every combination of selected values should become a run. |
| `runs.cases` | list | contextual | intermediate | contextual |  | Named run variants for runs.type=cases or runs.type=matrix. | Use this when runs need stable names or case-specific overrides. |
| `runs.cases[].axes` | mapping | contextual | advanced | contextual |  | Case-local grid axes for runs.type=matrix. | Use this when each named scenario needs its own sweep dimensions. |
| `runs.cases[].name` | value | contextual | intermediate | required for cases and matrix |  | Stable identifier used in expanded run ids. | Set this to a short, filesystem-safe name for each case. |
| `runs.cases[].set` | mapping | contextual | intermediate | {} |  | Dot-path overrides applied to a named run case. | Use this for hand-authored run variants with explicit settings. |
| `runs.type` | enum | contextual | common | single | `single`, `grid`, `cases`, `matrix` | Controls whether the config plans one run or expands a sweep. | Keep single for the starter; switch to grid, cases, or matrix when you need run expansion. |
| `dispatch.max_available_gpus` | value | contextual | intermediate | 1 |  | GPU budget used to serialize Slurm array groups when a plan exceeds available GPUs. | Set this to the practical cluster budget you want this workflow to consume. |
| `dispatch.overflow_policy` | enum | contextual | intermediate | serialize_groups | `serialize_groups`, `error`, `best_effort` | Controls planner behavior when run groups exceed the declared GPU budget. | Use error for strict admission control, or best_effort when the scheduler should absorb overflow. |
| `orchestration.controller` | value | contextual | advanced | partition=gpu, cpus=1, mem=2G, time_limit=01:00:00 |  | Slurm resources for the lightweight workflow controller job. | Change this when the controller needs a different partition, time limit, or environment. |
| `orchestration.controller.cpus` | integer | contextual | advanced | 1 |  | CPU count requested by the lightweight workflow controller job. | Increase this only if controller scheduling or planning overhead requires it. |
| `orchestration.controller.environment` | value | contextual | advanced | default |  | Environment profile loaded before the lightweight workflow controller runs. | Change this when controller jobs need cluster modules or setup scripts. |
| `orchestration.controller.mem` | value | contextual | advanced | 2G |  | Memory requested by the lightweight workflow controller job. | Increase this when controller planning or notification work needs more memory. |
| `orchestration.controller.partition` | value | contextual | advanced | gpu |  | Slurm partition for the lightweight train/eval controller job. | Change this when controller jobs need a different queue from stage jobs. |
| `orchestration.controller.time_limit` | duration | contextual | advanced | 01:00:00 |  | Slurm time limit requested by the lightweight workflow controller job. | Increase this when the controller must wait for long train/eval pipeline stages. |
| `stages.*.resources` | value | contextual | common | template-specific |  | Slurm partition, CPU, GPU, memory, and time settings for each stage. | Set these before real cluster runs so train and eval request the right resources. |
| `stages.*.resources.account` | value | contextual | intermediate | null |  | Slurm account charged for each stage task. | Set this when your cluster requires an account for GPU or partition access. |
| `stages.*.resources.constraint` | value | contextual | advanced | null |  | Raw Slurm constraint requested for each stage task. | Use this for cluster-specific node feature constraints that are not covered by gpu_type. |
| `stages.*.resources.cpus_per_task` | integer | contextual | common | template-specific |  | CPU count requested for each stage task. | Set this to match dataloader, preprocessing, or evaluation CPU needs. |
| `stages.*.resources.extra_sbatch_args` | list | contextual | advanced | [] |  | Additional raw sbatch arguments appended to each stage task script. | Use this for site-specific Slurm flags that do not have a first-class config field. |
| `stages.*.resources.gpu_type` | value | contextual | intermediate |  |  | Named GPU type from hardware.gpu_types used for GPU sizing and Slurm constraints. | Set this when a stage must target a specific accelerator family. |
| `stages.*.resources.gpus_per_node` | integer-or-auto | contextual | common | 1 |  | GPU count per node, or auto when GPU sizing is configured. | Set this to match the model memory and launcher requirements. |
| `stages.*.resources.mem` | value | contextual | common | template-specific |  | Memory requested for each stage task. | Set this to the memory required by model loading, training, or evaluation. |
| `stages.*.resources.nodes` | integer | contextual | intermediate | 1 |  | Number of nodes requested by each stage task. | Increase for multi-node launchers and distributed workloads. |
| `stages.*.resources.partition` | value | contextual | common | gpu |  | Slurm partition or queue for each stage task. | Set this to the queue appropriate for the stage workload. |
| `stages.*.resources.qos` | value | contextual | intermediate | null |  | Slurm QoS requested for each stage task. | Set this when your cluster uses QoS for priority, limits, or preemption policy. |
| `stages.*.resources.time_limit` | duration | contextual | common | 01:00:00 |  | Slurm time limit requested for each stage task. | Set this to the expected wall time plus a practical safety margin. |
| `stages.*.before` | list | contextual | advanced | [] |  | Commands run before the main stage entrypoint inside the stage task. | Use this for lightweight per-task setup that belongs with a stage rather than a reusable environment. |
| `stages.*.before[].name` | value | contextual | advanced |  |  | Optional label for a pre-entry setup command. | Set this when setup logs need a recognizable step name. |
| `stages.*.before[].run` | command | contextual | advanced | required when before[] is present |  | Shell command executed before the main stage entrypoint. | Use this for stage-local setup such as cache warmup or one-off file preparation. |
| `stages.*.enabled` | boolean | contextual | intermediate | true |  | Controls whether the stage participates in planning. | Disable a stage only when intentionally excluding it from this config. |
| `stages.*.entry.args` | value | contextual | common | template-specific |  | Mapping passed to each stage script as CLI flags. | Edit this for training hyperparameters, eval split names, data paths, or model flags. |
| `stages.*.entry.command` | command | contextual | intermediate | contextual |  | Shell command or argv list used when entry.type is command. | Use this for non-Python stages or wrappers that should run as a raw command. |
| `stages.*.entry.script` | path | contextual | common | template-specific |  | Script path resolved relative to the config file directory. | Point this at your real stage entrypoint. |
| `stages.*.entry.type` | enum | contextual | intermediate | python_script | `python_script`, `command` | Controls how the stage command is interpreted. | Use command when the entrypoint is a shell command instead of a Python script. |
| `stages.*.entry.workdir` | path | contextual | intermediate | . |  | Working directory used when launching the stage command. | Change this when scripts should run from a directory other than the config directory. |
| `stages.*.environment` | value | contextual | intermediate | default |  | Environment profile used before the stage runs. | Change this when a stage needs a different module/source/env setup. |
| `stages.*.kind` | value | contextual | advanced | template-specific |  | Stage role used by starter workflows and command selection. | Change only when defining a new stage role supported by the orchestration path. |
| `stages.*.runtime` | value | contextual | intermediate | default |  | User runtime profile used to execute the stage script. | Change this when a stage needs a different Python runtime or script environment. |
| `stages.eval.entry.script` | value | contextual | common | eval.py |  | Python script used for the eval stage. | Point this at your real evaluation entrypoint after replacing the generated demo code. |
| `stages.train.entry.script` | value | contextual | common | train.py |  | Python script used for the train stage. | Point this at your real training entrypoint after replacing the generated demo code. |
| `stages.*.launcher.args` | list | contextual | advanced | [] |  | Raw arguments passed to srun or mpirun launchers. | Use this for launcher-specific flags that are not modeled as first-class fields. |
| `stages.*.launcher.master_port` | port | contextual | advanced | null |  | Legacy torchrun master port override. | Prefer launcher.rendezvous.port for new configs; use this only for torchrun compatibility. |
| `stages.*.launcher.mode` | enum | contextual | advanced | single_node | `single_node`, `multi_node` | Declares whether launch happens on one node or across multiple nodes. | Use multi_node only when the selected launcher and resources are configured for multi-node jobs. |
| `stages.*.launcher.nnodes` | integer-or-auto | contextual | advanced | auto |  | Node count passed to distributed launchers, or auto from resources.nodes. | Set this explicitly when a launcher requires a concrete node count. |
| `stages.*.launcher.nproc_per_node` | integer-or-auto | contextual | advanced | auto |  | Process count per node passed to distributed launchers, or auto from resolved GPUs per node. | Set this when process count should differ from the resolved GPU count. |
| `stages.*.launcher.rendezvous` | mapping | contextual | advanced | backend=c10d, endpoint=auto, port=29500 |  | Distributed rendezvous settings for torchrun launchers. | Set this for multi-node torchrun jobs that need an explicit backend, endpoint, or port. |
| `stages.*.launcher.rendezvous.backend` | value | contextual | advanced | c10d |  | Rendezvous backend passed to torchrun. | Change this only when your distributed runtime requires a non-default backend. |
| `stages.*.launcher.rendezvous.endpoint` | value | contextual | advanced | auto |  | Rendezvous endpoint passed to torchrun. | Use auto for Slurm-managed multi-node jobs; set a value only for custom rendezvous wiring. |
| `stages.*.launcher.rendezvous.port` | port | contextual | advanced | 29500 |  | TCP port used by torchrun rendezvous. | Change this when the default port conflicts with site policy or another service. |
| `stages.*.launcher.srun_args` | list | contextual | advanced | [] |  | Raw srun arguments used by multi-node torchrun launch. | Use this for Slurm flags required by the torchrun bootstrap step. |
| `stages.*.launcher.type` | enum | contextual | advanced | single | `single`, `python`, `torchrun`, `srun`, `mpirun`, `command` | Chooses how a stage process is launched. | Keep single for normal scripts; use torchrun, srun, mpirun, or command for distributed/custom launch. |
| `stages.*.gpu_sizing` | mapping | contextual | advanced | null |  | Automatic GPU count sizing policy for a stage. | Use this when stages should derive GPU count from estimated memory instead of hard-coding gpus_per_node. |
| `stages.*.gpu_sizing.estimator` | value | contextual | advanced | required when gpu_sizing is present |  | Estimator name used to size a stage's GPU request. | Set this to the estimator implementation that knows how to estimate your stage workload. |
| `stages.*.gpu_sizing.max_gpus_per_job` | integer | contextual | advanced | null |  | Upper bound applied to automatic GPU sizing. | Set this to cap GPU use for cost, quota, or launcher limits. |
| `stages.*.gpu_sizing.min_gpus_per_job` | integer | contextual | advanced | 1 |  | Lower bound applied to automatic GPU sizing. | Increase this when launchers or workloads require a minimum GPU count. |
| `stages.*.gpu_sizing.round_to` | integer | contextual | advanced | null |  | Stage-specific GPU count granularity overriding sizing.gpu.defaults.round_to. | Use this when one stage needs a different GPU rounding policy. |
| `stages.*.gpu_sizing.safety_factor` | float | contextual | advanced | null |  | Stage-specific multiplier overriding sizing.gpu.defaults.safety_factor. | Use this when one stage needs a different memory safety margin. |
| `stages.*.gpu_sizing.target_memory_gb` | float | contextual | advanced | required when gpu_sizing is present |  | Estimated target memory in GB for a stage. | Set this to the stage's estimated peak GPU memory requirement. |
| `stages.*.depends_on` | list | contextual | intermediate | template-specific |  | Upstream stage names that must complete before this stage is selected. | Set this when a stage consumes outputs from earlier stages. |
| `stages.*.inputs.*.expects` | enum | contextual | intermediate | path | `path`, `manifest`, `value` | Declares the resolved input shape expected by the consuming stage. | Change this when passing a manifest payload or scalar value instead of a filesystem path. |
| `stages.*.inputs.*.inject.env` | value | contextual | intermediate | SFORGE_INPUT_CHECKPOINT |  | Environment variable used to pass the resolved input to the stage process. | Change this when the script expects a different environment variable name. |
| `stages.*.inputs.*.inject.flag` | value | contextual | common | checkpoint_path |  | CLI flag name used to pass the resolved input to the stage process. | Change this to match the argument name accepted by your script. |
| `stages.*.inputs.*.inject.mode` | enum | contextual | intermediate | path | `path`, `value`, `json` | Controls how a resolved input is injected into the stage process. | Use json for structured payloads or value for scalar values. |
| `stages.*.inputs.*.required` | boolean | contextual | intermediate | false |  | Controls whether a missing resolved input fails the stage plan. | Enable this for inputs that are mandatory for correct stage execution. |
| `stages.*.inputs.*.source.kind` | enum | contextual | intermediate | template-specific | `upstream_output`, `external_path` | Controls where a stage input is resolved from. | Use upstream_output for pipeline dependencies and external_path for user-provided files. |
| `stages.*.inputs.*.source.output` | value | contextual | intermediate | checkpoint |  | Producer output name resolved from the upstream stage. | Change this when the producer stage exposes a different output contract. |
| `stages.*.inputs.*.source.path` | path | contextual | common | checkpoint.pt |  | Explicit user-provided input path resolved relative to the config directory. | Replace the starter sample path with the real artifact path before submit. |
| `stages.*.inputs.*.source.stage` | value | contextual | intermediate | train |  | Producer stage name for an upstream output input. | Change this when the input should consume a different upstream producer. |
| `stages.*.outputs.*.discover.globs` | list | contextual | common | checkpoints/**/*.pt |  | Glob patterns evaluated under the stage run directory. | Change these when the stage writes artifacts under a different directory or filename pattern. |
| `stages.*.outputs.*.discover.select` | enum | contextual | intermediate | latest_step | `latest_step`, `first`, `last` | Selects one path from discovered output glob matches. | Use first or last when lexicographic ordering is the desired selection rule. |
| `stages.*.outputs.*.file` | path | contextual | common | eval/metrics.json |  | JSON file produced by the stage and read by output discovery. | Change this when the stage writes metrics or manifests to a different path. |
| `stages.*.outputs.*.json_path` | value | contextual | common | $.accuracy |  | JSONPath used to read a metric value from the output file. | Change this when the metric lives under a different JSON key. |
| `stages.*.outputs.*.kind` | enum | contextual | intermediate | template-specific | `file`, `files`, `metric`, `manifest` | Declares the shape of a managed stage output. | Use file/files for artifacts, metric for JSON scalar metrics, and manifest for manifest files. |
| `stages.*.outputs.*.required` | boolean | contextual | intermediate | false |  | Controls whether missing output discovery fails the stage result. | Enable this for artifacts or metrics required by downstream stages or run acceptance. |
| `stages.eval.inputs.checkpoint` | value | contextual | common | template-specific |  | Checkpoint input consumed by the eval stage. | For train-eval keep upstream_output; for eval-checkpoint replace the sample external path. |
| `stages.eval.outputs.accuracy` | value | contextual | common | eval/metrics.json $.accuracy |  | Declared accuracy metric read from eval/metrics.json. | Change this if evaluation writes a different metric file or JSON path. |
| `stages.train.outputs.checkpoint` | value | contextual | common | checkpoints/**/*.pt |  | Declared checkpoint contract produced by the train stage. | Change the discovery glob if your training code writes checkpoints somewhere else. |
| `notifications.email.enabled` | value | contextual | advanced | false |  | Enables email summary notifications for terminal workflow events. | Enable after sendmail works on the cluster and recipient addresses are configured. |
| `notifications.email.from` | value | contextual | advanced | slurmforge@localhost |  | Sender address used by email delivery. | Change this to the approved sender identity for your cluster. |
| `notifications.email.mode` | enum | contextual | advanced | summary | `summary` | Controls email content shape. | Keep summary unless a future notification mode is added. |
| `notifications.email.on` | enum | contextual | advanced | batch_finished | `batch_finished`, `train_eval_pipeline_finished` | Terminal workflow events that trigger email summaries. | Use batch_finished for stage batches and train_eval_pipeline_finished for controller pipelines. |
| `notifications.email.sendmail` | path | contextual | advanced | /usr/sbin/sendmail |  | Local sendmail-compatible binary used to deliver email. | Change this if the cluster exposes sendmail at a non-default path. |
| `notifications.email.subject_prefix` | value | contextual | advanced | SlurmForge |  | Prefix added to SlurmForge notification email subjects. | Change this to identify a project, team, or cluster in inboxes. |
| `notifications.email.to` | value | contextual | advanced | [] |  | Recipients for email notifications when email is enabled. | Set this before enabling notifications. |
<!-- CONFIG_SCHEMA_REFERENCE_END -->

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
