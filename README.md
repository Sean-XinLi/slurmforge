# slurmforge

[![PyPI version](https://img.shields.io/pypi/v/slurmforge.svg)](https://pypi.org/project/slurmforge/)

## TL;DR

Define experiments in YAML → generate reproducible Slurm jobs.

```bash
sforge init
sforge validate
sforge generate
sbatch runs/.../sbatch/*.sh
```


`slurmforge` is a Slurm-native experiment orchestration toolkit designed for large-scale training workflows.

It helps you:
- expand experiment sweeps from a single config
- generate reproducible Slurm batch jobs
- manage training + evaluation pipelines with minimal boilerplate

It takes one experiment config, expands a sweep, resolves train and eval commands, groups runs by final Slurm resource shape, and materializes the batch records and sbatch files needed for execution.


## Why slurmforge? 

Compared to ad-hoc bash scripts or manual sbatch workflows:

- structured experiment definition (YAML instead of shell glue)
- deterministic sweep expansion
- built-in retry and replay support
- explicit separation of planning vs execution

Unlike general-purpose orchestration tools, slurmforge is designed specifically for Slurm environments.


## Architecture (High-Level)

```mermaid

flowchart TD
    A[Config YAML] --> B[Planning - Build Run Graph]
    B --> C[Materialization - Generate sbatch]
    C --> D[Execution - Runtime helpers]
    D --> E[Slurm Cluster]
    E --> F[Results / Logs]
```

This separation ensures reproducibility and easier debugging.


## Who This Is For

The following section is intended for users who are new to slurmforge.

You do not need to understand the internal planner or executor model to start. The intended workflow is:

1. keep your real training code in your own project directory
2. generate a starter project with `sforge init`
3. either edit the generated starter scripts or point the config at your existing train and eval entrypoints
4. run `sforge validate`
5. run `sforge generate`
6. submit the generated `sbatch` files


## Install

Install from PyPI:

```bash
pip install slurmforge
```

Or install from source for the latest development version:

```bash
git clone https://github.com/Sean-XinLi/slurmforge
cd slurmforge
python -m venv ../slurmforge_venv
source ../slurmforge_venv/bin/activate
pip install .
```

Main CLI:

```bash
sforge --help
```

Most users only need `sforge`. The low-level runtime helpers are invoked automatically by generated batch scripts.

## Quick Start

The recommended newcomer path is `init`.

Create a starter project scaffold (interactive wizard):

```bash
sforge init
```

Or specify type and profile directly:

```bash
sforge init script --out ./demo_project
cd ./demo_project
```

Validate the config first:

```bash
sforge validate --config ./experiment.yaml
```

Preview the generated batch:

```bash
sforge generate --config ./experiment.yaml --dry_run
```

Generate the batch files:

```bash
sforge generate --config ./experiment.yaml
```

Generated batches persist the `slurmforge` version that planned them.
After upgrading `slurmforge`, older batches may still execute or replay with compatibility warnings instead of a hard stop.
For new submissions after an upgrade, regenerate the batch so planning and execution use the same installed version.

Then submit the generated Slurm scripts under:

```text
runs/<project>/<experiment>/batch_<name>/sbatch/
```

## Connect A Starter To Your Code

There are two normal ways to adapt a starter project:

1. Replace the generated `train.py`, `eval.py`, or `train_adapter.py` bodies with your real logic.
2. Keep the generated `experiment.yaml`, but change `model.script` and `eval.script` to point at entrypoint scripts that already exist in your project.

`model.script` should point to the script that launches training, not to a module that only defines layers or model classes.

Typical direct-entrypoint edit:

```yaml
model:
  name: "my_model"
  script: "train.py"

eval:
  enabled: true
  script: "eval.py"
```

Typical existing-project edit:

```yaml
model:
  name: "my_model"
  script: "src/train_my_model.py"

eval:
  enabled: true
  script: "tools/run_eval.py"
```

If you use `model_cli`, make sure the script named by `model.script` accepts the arguments declared under `run.args`.

## Starter Modes

Use `init` when you want a starter project scaffold.

`init` takes two orthogonal choices: **type** (how your training code is invoked) and **profile** (cluster complexity).

```bash
sforge init                          # interactive wizard
sforge init script                   # script type, starter profile (default)
sforge init script   --profile hpc   # script type, hpc profile
sforge init command
sforge init command  --profile hpc
sforge init registry
sforge init registry --profile hpc
sforge init adapter
sforge init adapter  --profile hpc
```

Types:
- `script` — train.py-style script; slurmforge manages args and submission
- `command` — wraps a complete shell command in Slurm
- `registry` — uses a shared team model registry
- `adapter` — interface bridge script (advanced)

Profiles:
- `starter` — single GPU, minimal config; runnable immediately after filling in 4 fields
- `hpc` — multi-GPU, sweep, eval, artifact sync; includes placeholders for cluster account, environment activation, and data paths that you replace before execution

Typical generated files:

- `experiment.yaml`
- `README.md`
- `runs/`
- type-specific files such as `train.py`, `eval.py`, `train_adapter.py`, or `models.yaml`

Run `sforge init --help` to see the full usage.

## Raw YAML References

Use `examples` when you want to inspect or export the raw YAML reference files.

List available examples:

```bash
sforge examples list
```

Show one example:

```bash
sforge examples show script_hpc
```

Export one example:

```bash
sforge examples export script_hpc --out ./experiment.yaml
```

`examples` is the raw YAML layer. `init` is the recommended starter-project layer built around those YAML definitions.

## Runtime Internals

`sforge-run-plan-executor`, `sforge-artifact-sync`, `sforge-write-train-outputs`, and `sforge-write-attempt-result` are low-level runtime helpers.

Most users do not call them directly. Generated batch scripts and debugging workflows use them to execute one run record, resolve train outputs for eval handoff, collect artifacts into the result directory, and persist structured `attempt_result.json` metadata after train/eval finishes.

## Core Commands

Validate a config without generating a batch:

```bash
sforge validate --config /path/to/experiment.yaml
```

Generate a batch:

```bash
sforge generate --config /path/to/experiment.yaml
```

Preview without writing files:

```bash
sforge generate --config /path/to/experiment.yaml --dry_run
```

Override config values from the CLI:

```bash
sforge generate \
  --config /path/to/experiment.yaml \
  --set run.args.lr=0.003 \
  --set cluster.mem=80G
```

Retry failed runs from an existing batch:

```bash
sforge rerun --from /path/to/batch_root
```

Replay a specific persisted run:

```bash
sforge replay --from-run /path/to/batch_root/runs/run_001_abcd1234
```

Replay directly from a snapshot file:

```bash
sforge replay --from-snapshot /path/to/run_snapshot.json
```

Replay selected runs from a batch:

```bash
sforge replay --from-batch /path/to/batch_root --run_id r1 --run_id r2
```

Replay selected runs by both id and index:

```bash
sforge replay --from-batch /path/to/batch_root --run_id r1 --run_index 1
```

`replay --from-batch` replays every run by default.
Repeat `--run_id` or `--run_index` to narrow the selection.
If you pass both flags, slurmforge uses intersection semantics: a run must match the selected ids and the selected indices.

When retries find a checkpoint under the previous run's result directory, the rebuilt run will:

- export `AI_INFRA_RESUME_FROM_CHECKPOINT`
- pass `--resume_from_checkpoint ...` for structured modes that slurmforge controls

Checkpoint resume selection is deterministic, not heuristic:

- if `job-*/meta/checkpoint_state.json` exists, `rerun` uses it as the authoritative latest checkpoint pointer
- otherwise slurmforge scans discovered checkpoint files and selects the highest parseable step number from the filename
- if multiple checkpoint candidates exist and none expose a parseable step number, `rerun` fails instead of guessing from file modification time

In practice, that means your training outputs should do one of these:

- update `job-*/meta/checkpoint_state.json` whenever a new latest checkpoint is committed
- or name checkpoint files with a stable step number such as `global_step_1200`, `step1200`, `checkpoint-1200`, or `ckpt_1200`

Use `replay` when you want an exact user-directed replay source.
Use `rerun` when you want status-based retry selection plus automatic checkpoint resume injection.

Inspect run status:

```bash
sforge status --from /path/to/batch_root
```

If `squeue` / `sacct` are available on the machine where you run `status`, slurmforge will use Slurm-native job states to distinguish `pending`, `running`, and terminal scheduler states before falling back to local logs.

## Path Rules

- `--config` is required for `validate` and `generate`
- relative paths inside the config resolve against `project_root`
- by default, `project_root` is the directory that contains the config file
- `--project_root` lets you override that explicitly
- `validate` and `generate` use the same `--set` and `--project_root` semantics
- `replay` restores the original planning root from persisted metadata; if the project moved, pass `--project_root`
- `rerun` restores the original planning root from persisted run metadata; if the project moved, pass `--project_root`

Fields typically resolved relative to `project_root`:

- `model_registry.registry_file`
- `model.script`
- `model.yaml`
- `launcher.workdir`
- `run.workdir`
- `run.adapter.script`
- `eval.script`
- `eval.workdir`
- `output.base_output_dir`

## Choosing A Train Mode

The package supports three internal train modes, each corresponding to an `init` type:

- `command` (`sforge init command`): run an existing command exactly as provided; slurmforge does not rewrite it into `torchrun` or infer a distributed launcher topology from it
- `model_cli` (`sforge init script` or `sforge init registry`): build the train command from `model` and `run.args`
- `adapter` (`sforge init adapter`): call a bridge script that translates slurmforge inputs to some external system

Recommended order for new users:

1. `sforge init command` if you only want to wrap an existing command quickly
2. `sforge init script` as the default structured path
3. `sforge init registry` when a team wants a shared model catalog
4. `sforge init adapter` only for advanced or non-standard integrations

If you use `model.script` directly, the default assumption is `ddp_supported: true`. Set `model.ddp_supported: false` explicitly for single-process-only scripts.

Use `command` mode only when your command text already expresses the launcher you want. If you need slurmforge to manage `torchrun`, GPU process counts, or multi-node Slurm launch details, use `script` or `adapter` init types.

## Advanced Configuration

### Hyperparameter Sweep

`sweep` generates the matrix product of all declared axes.
Each combination becomes one independent Slurm task.

**Flat grid (shared_axes only):**

```yaml
sweep:
  enabled: true
  max_runs: 20            # optional cap on total runs
  shared_axes:
    run.args.lr:          [1e-4, 5e-5, 1e-5]
    run.args.batch_size:  [64, 128]
```

**Named cases** — each case can have its own fixed values (`set`) and additional axes:

```yaml
sweep:
  enabled: true
  shared_axes:
    run.args.lr: [1e-4, 5e-5]
  cases:
    - name: "case_1"
      set:
        run.args.optimizer: "adam"
    - name: "case_2"
      set:
        run.args.optimizer: "sgd"
      axes:
        run.args.epochsize: [10, 20, 40]
```

Each case is multiplied with `shared_axes` independently, so the total runs equal
`len(shared_axes_product) × sum(len(case_product) for each case)`.

`max_runs` truncates the final expansion deterministically if set.

Dot-path keys in `shared_axes`, `set`, and `axes` must not overlap within or across a case.

---

### Inline Evaluation

`eval` runs inside the same Slurm job immediately after training completes.

```yaml
eval:
  enabled: true
  script: "eval.py"
  workdir: "."
  launch_mode: "inherit"   # auto / ddp / single / inherit (inherit = use same launcher as train)
  pass_run_args: true       # pass run.args to eval script as --run_args_json
  run_args_flag: "run_args_json"
  pass_model_overrides: false
  model_overrides_flag: "model_overrides_json"
  args:                     # extra eval-only args
    test_split: 0.02
  launcher:
    distributed:
      master_port: 29900    # separate port to avoid conflict with train launcher
      extra_torchrun_args: []
  train_outputs:
    checkpoint_policy: "latest"   # latest / best / explicit
    # explicit_checkpoint: "checkpoints/step_5000.pt"  # only when policy=explicit
```

`eval.command` can be used instead of `eval.script` for an arbitrary shell command.
When using `eval.command`, `eval.external_runtime` is required and `eval.args`/`pass_run_args`/`pass_model_overrides` are not available.

---

### Email Notifications

```yaml
notify:
  enabled: true
  email: "you@example.com"
  when: "afterany"    # after / afterany / afterok / afternotok
```

`when` uses Slurm dependency vocabulary: `afterany` sends on any completion,
`afterok` only on success, `afternotok` only on failure.

---

### Automatic GPU Allocation

When `resources.auto_gpu: true`, slurmforge estimates the GPU count per job
from model memory heuristics and sets `cluster.gpus_per_node` automatically.

```yaml
resources:
  auto_gpu: true
  gpu_estimator: "heuristic"
  target_mem_per_gpu_gb: 80    # target memory per GPU in GB
  safety_factor: 1.15          # multiply estimated memory by this factor (>= 1.0)
  min_gpus_per_job: 1
  max_gpus_per_job: 8
  max_available_gpus: 8

cluster:
  gpus_per_node: "auto"        # set to "auto" to let resources block drive this
```

---

### Distributed Launcher

Full `torchrun`-based distributed config:

```yaml
launcher:
  mode: "auto"          # auto selects ddp when ddp_supported=true and gpus_per_node > 1
  python_bin: "python3"
  workdir: "."
  distributed:
    nnodes: 1
    nproc_per_node: "auto"      # int or "auto" (matches gpus_per_node)
    master_port: 29500
    port_offset: "auto"         # int or "auto" (avoids port collisions across array tasks)
    extra_torchrun_args:
      - "--rdzv_backend=c10d"
      - "--max_restarts=2"
```

Set `model.ddp_supported: false` to force `single` mode regardless of GPU count.
Set `model.ddp_required: true` to fail fast if DDP cannot be selected.

---

### Cluster Configuration

```yaml
cluster:
  partition: "your_partition"
  account: "my_account"
  qos: "high_priority"         # optional QoS override
  time_limit: "04:00:00"       # or "2-00:00:00" for 2 days
  nodes: 1
  gpus_per_node: 4
  cpus_per_task: 8
  mem: "64G"                   # "0" = unlimited
  constraint: "a100|h100"      # optional node constraint
  extra_sbatch_args:            # raw #SBATCH directives
    - "--exclude=node001,node002"
    - "--reservation=my_reservation"
```

---

### Cross-Batch Slurm Dependencies

`output.dependencies` injects `--dependency` flags into every generated array job,
allowing you to chain batches without manual sbatch calls.

```yaml
output:
  base_output_dir: "./runs"
  batch_name: "finetune_v2"
  dependencies:
    afterok:
      - "4512345"    # Slurm job IDs from a previous batch
      - "4512346"
    afterany:
      - "4512347"
```

Supported dependency types: `after`, `afterany`, `afterok`, `afternotok`.

---

### Artifact Collection

slurmforge collects artifacts from the run working directory into the result directory after each job.

```yaml
artifacts:
  checkpoint_globs:
    - "checkpoints/**/*.pt"
    - "checkpoints/**/*.ckpt"
  eval_csv_globs:
    - "eval_csv/**/*.csv"
  eval_image_globs:
    - "eval_images/**/*.png"
    - "eval_images/**/*.pdf"
  extra_globs:
    - "logs/**/*.log"
```

---

### Validation Policies

Control how slurmforge handles various warnings and errors:

```yaml
validation:
  cli_args: "warn"          # warn / error / ignore — unknown CLI args in run.args
  topology_errors: "error"  # error / warn / off    — DDP topology mismatches
  resource_warnings: "warn" # warn / error / off    — GPU/memory estimation warnings
  runtime_preflight: "error"# error / warn / off    — script existence checks
```

---

### Command Mode with External Runtime

Use `command` mode to wrap an arbitrary shell command.
`external_runtime` declares the topology slurmforge uses when injecting the command into a Slurm array.

```yaml
run:
  command: "bash scripts/train.sh --config cfg.yaml"
  command_mode: "argv"      # argv (shell-escaped) / raw (shell expansion enabled)
  external_runtime:
    nnodes: 1
    nproc_per_node: 4
```

`command_mode: raw` passes the command string to bash without escaping — useful for pipes and
redirects, but disables slurmforge's argument safety checks.

---

### Adapter Mode

`adapter` mode calls a bridge script that translates slurmforge's structured inputs to an
external training system.

```yaml
run:
  adapter:
    script: "train_adapter.py"
    pass_run_args: true
    run_args_flag: "run_args_json"
    pass_model_overrides: true
    model_overrides_flag: "model_overrides_json"
    ddp_supported: false
    ddp_required: false
  args:
    lr: 0.004

launcher:
  mode: "auto"
```

The adapter script receives `run.args` as a JSON blob via `--run_args_json` and
`run.model_overrides` via `--model_overrides_json`.

## Notes

- batch materialization is always array-based in the current contract; `output.dispatch_mode` has been removed
- `output.dependencies` can add external Slurm dependencies such as `afterok` or `afterany` to every generated array job when you need cross-batch sequencing
- `notify.when` uses the same Slurm dependency vocabulary as batch submission dependencies
- `eval` currently runs inline inside the same generated job as `train`; `output.dependencies` is a batch-level Slurm dependency feature, not a per-run train→eval stage DAG
- `eval.train_outputs` controls how slurmforge selects the checkpoint handed off from train to eval; it must be a mapping, e.g. `{checkpoint_policy: latest}`; supported policies are `latest`, `best`, and `explicit`
- `sweep` is always matrix expansion; valid top-level keys are `enabled`, `max_runs`, `shared_axes`, and `cases`; there is no `sweep.method` or `sweep.params` key
- your train and eval scripts must exist on a Slurm-visible filesystem
- generated array jobs bootstrap `env.modules`, `env.conda_activate`, and `env.venv_activate` before invoking `sforge-run-plan-executor`; that activated runtime environment must expose `sforge-run-plan-executor`, `sforge-artifact-sync`, `sforge-write-train-outputs`, and `sforge-write-attempt-result` on compute nodes
- `generate` persists run metadata so `rerun` can replay without package-local path guesses
- `eval` artifact fallback scans both train and eval workdirs

## Maintenance Policy

This project is currently maintained on a best-effort basis.  
Responses to issues and pull requests may be delayed.

Pull requests are welcome for:
- bug fixes
- documentation improvements

New features may not be accepted unless aligned with the project scope.



## Development

```bash
pip install -e '.[dev]'
pytest -q
```

## Author and Maintainer

Created and maintained by Xin Li.

- Email: `seanxinlee@gmail.com`
- GitHub: <https://github.com/Sean-XinLi>
