# slurmforge

`slurmforge` is a Slurm-native stage-batch system for AI training and evaluation workflows.

It focuses on a small CLI surface:

```bash
sforge init
sforge init --template train-eval --output experiment.yaml --force
sforge init --list-templates
sforge validate --config experiment.yaml
sforge estimate --config experiment.yaml
sforge plan train --config experiment.yaml --dry-run=full --output plan.audit.json
sforge plan eval --config experiment.yaml --checkpoint /path/to/model.pt --input-name model_input
sforge plan run --config experiment.yaml
sforge train --config experiment.yaml --dry-run=full
sforge eval --config experiment.yaml --checkpoint /path/to/model.pt
sforge run --config experiment.yaml
sforge status --from /path/to/root --reconcile
sforge resubmit --from /path/to/root --stage eval --query state=failed
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Start

Create a starter project instead of writing YAML from scratch:

```bash
sforge init
```

For scripts or CI, choose a template explicitly:

```bash
sforge init --template train-eval --output experiment.yaml --force
```

Available starter templates:

- `train-eval`: train produces a checkpoint; eval consumes the upstream output.
- `train-only`: one train stage with a checkpoint output.
- `eval-checkpoint`: one eval stage that consumes an explicit checkpoint path.

## Minimal Workflow

```bash
sforge validate --config experiment.yaml
sforge run --config experiment.yaml --dry-run=full
sforge run --config experiment.yaml --emit-only
sforge run --config experiment.yaml
sforge status --from ./runs/<project>/<experiment>/<pipeline-root> --reconcile
```

Use `sforge train` for train-only configs and `sforge eval --checkpoint /path/to/model.pt` for eval-only configs.

## Docs

- [Quickstart](docs/quickstart.md)
- [Config Reference](docs/config.md)
- [Operations](docs/operations.md)
- [Internals](docs/internals.md)

## Development

```bash
PYTHONPATH=src python3.11 -m pytest -q
PYTHONPATH=src lint-imports --config .importlinter
```
