# Submission Records

## Input Contract

Every stage instance has `input_bindings.json`:

```json
{
  "schema_version": 1,
  "stage_instance_id": "run_001.eval",
  "bindings": {
    "checkpoint": {
      "source": {
        "kind": "upstream_output",
        "stage": "train",
        "output": "checkpoint"
      },
      "expects": "path",
      "resolved": {
        "kind": "path",
        "path": "/abs/path/checkpoints/step_12000.pt",
        "digest": "sha256..."
      },
      "inject": {
        "flag": "checkpoint_path",
        "env": "SFORGE_INPUT_CHECKPOINT",
        "mode": "path"
      },
      "resolution": {
        "kind": "upstream_output",
        "producer_root": "/abs/path/train_batch",
        "producer_run_dir": "/abs/path/train_batch/runs/run_001",
        "producer_stage_instance_id": "run_001.train",
        "producer_run_id": "run_001",
        "producer_stage_name": "train",
        "output_name": "checkpoint",
        "output_path": "/abs/path/checkpoints/step_12000.pt",
        "output_digest": "sha256...",
        "selection_reason": "latest_step"
      }
    }
  }
}
```

The binding file is the source of truth. Environment variables and CLI flags are derived injection forms. `checkpoint` is only a conventional name in examples; eval may use any input name if the source and injection contract are explicit.

The YAML input contract is explicit:

```yaml
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
```

Allowed source kinds are `upstream_output` and `external_path`. CLI shortcuts such as `--checkpoint` compile to `external_path` bindings with `resolution.source_role=checkpoint`; checkpoint is not a separate core source kind.

`resolved` is the typed resolved value consumed by injection and verification. It may be `path`, `value`, `manifest`, or `unresolved`. For `path` and `manifest`, `resolved.path` is the injected filesystem value. For `value`, `resolved.value` is injected by `mode: value` or serialized by `mode: json`. `resolution` is the audit trail for how the binding was resolved: producer root, producer stage instance, output name, digest, and any lineage lookup used to recover the source.

## Materialization Contract

Every stage batch root writes `materialization_status.json`:

```json
{
  "schema_version": 1,
  "batch_id": "eval_batch_xxx",
  "stage_name": "eval",
  "state": "ready",
  "failure_class": null,
  "reason": "",
  "verified_at": "2026-01-01T00:00:00+00:00",
  "submit_manifest_path": "/abs/path/submit/submit_manifest.json"
}
```

States:

- `planned`: batch layout and logical plans exist.
- `verifying_inputs`: required input readiness checks are running.
- `ready`: submit files and submission ledger are available.
- `blocked`: the batch cannot be submitted because a contract check failed.

`blocked` is readiness failure, not execution failure. It does not create an attempt. The affected stage instances move to `StageStatusRecord.state=blocked` with `failure_class=input_contract_error`.

Submission has one public write path: `prepare_stage_submission(batch)` returns a `PreparedSubmission`, and `submit_prepared_stage_batch(prepared)` is the only public submit entrypoint. Low-level ledger writes, event appends, and stage sbatch generation are private package internals.
