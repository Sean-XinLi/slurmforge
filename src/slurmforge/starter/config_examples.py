from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from ..defaults import TEMPLATE_TRAIN_EVAL
from . import InitRequest
from .catalog import get_template
from .config_yaml import render_starter_config


def render_starter_example(project_root: Path) -> str:
    template = get_template(TEMPLATE_TRAIN_EVAL)
    return render_starter_config(
        TEMPLATE_TRAIN_EVAL,
        template.config_builder(
            InitRequest(template=TEMPLATE_TRAIN_EVAL, output_dir=project_root)
        ),
    )


def render_advanced_example() -> str:
    return (
        dedent(
            """
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
                  estimator: heuristic
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
            """
        ).strip()
        + "\n"
    )
