# Artifact And Lineage Records

## Lineage Contract

Every stage batch and train/eval pipeline root writes `lineage_index.json`.

Stage batch lineage records:

- `batch_id`
- `stage_name`
- `source_ref`
- `run_ids`
- `stage_instances`
- `source_roots`
- `input_sources`

`input_sources` repeats each persisted input binding with its `resolution`. This is the durable join point between an eval batch and the train batch that produced its checkpoint.

Train/eval pipeline lineage records:

- `pipeline_id`
- `pipeline_kind = train_eval_pipeline`
- `stage_order`
- `run_ids`
- `stage_batches`
- `source_roots`

Lineage is used by `resubmit` after replanning the target stage. For example, `sforge resubmit --from <eval_batch_root> --stage eval` can resolve the checkpoint from the eval batch's own lineage index even when the train batch root is not the command target.

Each materialized resubmit reserves a fresh `<source_root>/derived_batches/<batch_id>` directory and writes `source_plan.json` plus `source_lineage.json` before input verification or sbatch generation. A failed readiness check leaves an auditable source contract, not a hidden half-plan.

## Output Contract

Every `stage_plan.json` carries a schema-versioned typed `output_contract`. Checkpoints are conventional file outputs with discovery globs and a selection policy; declared multi-file outputs, metrics, and manifests are represented by first-class output contracts. Executors consume this typed contract directly.

Every successful stage instance has `stage_outputs.json`:

```json
{
  "schema_version": 1,
  "stage_instance_id": "train/run_001",
  "outputs": {
    "checkpoint": {
      "kind": "file",
      "path": "/abs/path/attempts/0001/artifacts/files/abc123_step_12000.pt",
      "source_path": "/abs/path/checkpoints/step_12000.pt",
      "managed": true,
      "strategy": "copy",
      "digest": "sha256...",
      "source_digest": "sha256...",
      "managed_digest": "sha256...",
      "verified": true,
      "producer_stage_instance_id": "train/run_001",
      "producer_attempt_id": "0001",
      "selection_reason": "latest_step"
    },
    "accuracy": {
      "kind": "metric",
      "path": "/abs/path/attempts/0001/artifacts/files/abc123_metrics.json",
      "source_path": "/abs/path/metrics/accuracy.json",
      "managed": true,
      "digest": "sha256...",
      "source_digest": "sha256...",
      "managed_digest": "sha256...",
      "verified": true,
      "value": 0.98,
      "producer_stage_instance_id": "eval/run_001",
      "producer_attempt_id": "0001",
      "selection_reason": "json_path:$.accuracy"
    },
    "eval_report": {
      "kind": "manifest",
      "path": "/abs/path/attempts/0001/artifacts/files/eval_manifest.json",
      "source_path": "/abs/path/eval/manifest.json",
      "managed": true,
      "producer_stage_instance_id": "eval/run_001",
      "producer_attempt_id": "0001",
      "selection_reason": "manifest_file"
    }
  },
  "artifacts": ["/abs/path/attempts/0001/artifacts/files/def456_train.log"],
  "artifact_manifest": "/abs/path/attempts/0001/artifacts/artifact_manifest.json"
}
```

Downstream stages consume explicit `OutputRef` values resolved into `InputBinding` records.

Every successful attempt writes an artifact manifest:

```json
{
  "schema_version": 1,
  "stage_instance_id": "train/run_001",
  "attempt_id": "0001",
  "artifacts": [
    {
      "name": "checkpoint",
      "kind": "file",
      "source_path": "/abs/path/checkpoints/step_12000.pt",
      "managed_path": "/abs/path/attempts/0001/artifacts/files/abc123_step_12000.pt",
      "strategy": "copy",
      "managed": true,
      "digest": "sha256...",
      "source_digest": "sha256...",
      "managed_digest": "sha256...",
      "verified": true,
      "size_bytes": 123,
      "optional": false
    }
  ]
}
```
