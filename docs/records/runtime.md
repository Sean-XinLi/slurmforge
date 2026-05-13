# Runtime Records

## Runtime Objects

Runtime state is stage-scoped:

- `StageAttemptRecord`
- `StageStatusRecord`
- `RunStatusRecord`
- `TrainEvalPipelineStatusRecord`

`StageAttemptRecord` records one stage attempt and one stage exit code:

- `attempt_id`
- `stage_instance_id`
- `attempt_source = executor | scheduler_reconcile`
- `attempt_state = starting | running | final | reconciled`
- `scheduler_job_id`
- `scheduler_array_job_id`
- `scheduler_array_task_id`
- `scheduler_state`
- `scheduler_exit_code`
- `node_list`
- `started_by_executor`
- `executor_started_at`
- `executor_finished_at`
- `started_at`
- `finished_at`
- `exit_code`
- `failure_class`
- `reason`
- `log_paths`
- `artifact_paths`
- `artifact_manifest_path`

`StageStatusRecord` is the atomic status record:

- `state = planned | queued | running | success | failed | cancelled | skipped | blocked`
- `latest_attempt_id`
- `latest_output_digest`
- `failure_class`
- `reason`

Run and train/eval pipeline status are derived from stage status records.

Status writes use monotonic transitions. Normal queued/running markers cannot move a terminal stage back to a non-terminal state.

Every status transition appends a line to `status_events.jsonl` in the run directory.

Every materialized run directory writes `root_ref.json`. It records the containing stage batch root and, when applicable, the parent train/eval pipeline root. Stage status commits stay per-stage; layout writes, control gate progression, and `status --reconcile` refresh aggregate `run_status.json` / `train_eval_pipeline_status.json` from the current stage records.

Every executor attempt writes `runtime_probe.json` under the attempt directory. It is a `RuntimeContractReport` with a single `state`, `failure_reason`, and both executor and user Python probes. A failed report prevents user code launch.

Runtime plans use the same nested runtime shape as the spec:

```json
{
  "executor": {
    "python": {
      "bin": "python3.11",
      "min_version": "3.10"
    },
    "module": "slurmforge.executor.stage"
  },
  "user": {
    "name": "default",
    "python": {
      "bin": "python3.11",
      "min_version": "3.10"
    }
  }
}
```

## Input Verification Contract

Every executable stage instance writes `input_verification.json` before sbatch generation and again before executor launch.

```json
{
  "schema_version": 1,
  "stage_instance_id": "eval/run_001",
  "run_id": "run_001",
  "stage_name": "eval",
  "phase": "submit",
  "state": "verified",
  "records": [
    {
      "input_name": "checkpoint",
      "source": {
        "schema_version": 1,
        "kind": "upstream_output",
        "stage": "train",
        "output": "checkpoint",
        "path": ""
      },
      "expects": "path",
      "resolved_kind": "path",
      "resolved_path": "/abs/path/checkpoints/step_12000.pt",
      "required": true,
      "path_kind": "file",
      "exists": true,
      "readable": true,
      "size_bytes": 123,
      "expected_digest": "sha256...",
      "producer_digest": "sha256...",
      "producer_stage_instance_id": "train/run_001",
      "digest": "sha256...",
      "value_digest": "",
      "verified_at": "2026-01-01T00:00:00+00:00",
      "phase": "submit",
      "state": "verified",
      "failure_class": null,
      "reason": "verified"
    }
  ]
}
```

Verification phases:

- `submit`: runs before executable sbatch files and submission ledger are generated.
- `executor`: runs on the compute node before user code is launched.

Required input rules:

- required inputs must satisfy the typed `resolved` contract: `path`/`manifest` need `resolved.path`; `value` needs `resolved.kind=value`;
- filesystem-backed inputs must exist;
- filesystem-backed inputs must be readable;
- inputs with `expected_digest` must match the current file digest;
- `upstream_output` and `external_path` default to file verification;
- failure is `input_contract_error`;
- executor preflight failure must not launch user code.

Normal submit/executor verification records `expected_digest` from lineage when available. When an expected digest is present, verification hashes the current file and rejects mismatches as `input_contract_error`.
