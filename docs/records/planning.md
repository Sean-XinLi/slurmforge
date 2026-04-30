# Planning Records

## Planning Objects

The core planning objects are:

- `RunDefinition`
- `StageInstancePlan`
- `StageBatchPlan`
- `TrainEvalPipelinePlan` (`pipeline_kind=train_eval_pipeline`)
- `InputBinding`
- `OutputRef`

`RunDefinition` is the planned run identity produced by `runs`. A single run, each grid point, each named case, or each matrix case grid point has one stable `run_id`.

`StageInstancePlan` is the executable plan for one stage of one run:

- `stage_instance_id = <run_id>.<stage_name>`
- `run_id`
- `stage_name`
- `stage_kind`
- `entry`
- `resources`
- `resource_sizing`
- `runtime_plan`
- `launcher_plan`
- `artifact_store_plan`
- `input_bindings`
- `output_contract`
- `lineage`

`StageBatchPlan` groups same-stage instances by resource shape and emits Slurm array jobs. A group never mixes train and eval.

`TrainEvalPipelinePlan` is train/eval pipeline orchestration metadata. It is consumed by short-lived control gates and does not execute user code directly.

The supported topology is deliberately narrow: `train`, `eval`, or `train -> eval`. Stage-batch v1 is not an arbitrary DAG engine.

Current persisted plan loaders are strict. Required fields are read by key, not defaulted during load:

- `StageInstancePlan.resource_sizing`
- `GroupPlan.stage_instance_ids`, `run_ids`, `array_throttle`, and `gpus_per_task`
- `StageBatchPlan.selected_runs`, `stage_instances`, `group_plans`, `source_ref`, `budget_plan`, and `notification_plan`
- `TrainEvalPipelinePlan.pipeline_kind`, `stage_order`, `run_set`, `stage_batches`, and `notification_plan`
- `TrainEvalControlPlan.pipeline_kind`, `stage_order`, `resources`, `environment_name`, `environment_plan`, and `runtime_plan`

A missing field means the persisted file is outside the current schema and must fail fast instead of being silently upgraded at read time.
