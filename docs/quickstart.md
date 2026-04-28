# Quickstart

`slurmforge` plans and submits train/eval stage batches on Slurm. The common path is to generate a starter project, validate the config, inspect the full dry-run audit, then submit.

## Create A Starter

Interactive:

```bash
sforge init
```

Non-interactive:

```bash
sforge init --template train-eval --output experiment.yaml --force
```

Template choices:

- `train-eval`: train produces a checkpoint and eval consumes it through `upstream_output`.
- `train-only`: train produces a checkpoint and stops.
- `eval-checkpoint`: eval consumes an explicit checkpoint path. The starter writes `checkpoint.pt` as sample input.

`sforge init` is intentionally narrow. It only selects the template, chooses the output path, and confirms overwrite behavior.

## Validate And Preview

For a train/eval pipeline:

```bash
sforge validate --config experiment.yaml
sforge run --config experiment.yaml --dry-run=full
```

For train-only:

```bash
sforge validate --config experiment.yaml
sforge train --config experiment.yaml --dry-run=full
```

For eval from a checkpoint:

```bash
sforge validate --config experiment.yaml
sforge eval --config experiment.yaml --checkpoint checkpoint.pt --dry-run=full
```

Relative `--checkpoint` paths are resolved from the config directory.

`--dry-run=full` compiles the plan, verifies contracts, probes the declared runtimes, and includes a `resource_estimate` block in the machine-readable audit.

## Submit

```bash
sforge run --config experiment.yaml
```

Use `--emit-only` to write plans, sbatch files, manifests, and a planned submission ledger without calling `sbatch`.

## Inspect Status

```bash
sforge status --from ./runs/<project>/<experiment>/<root>
sforge status --from ./runs/<project>/<experiment>/<root> --reconcile
```

`--reconcile` queries Slurm through the persisted submission ledger and refreshes status projections before printing.
