from __future__ import annotations

from pathlib import Path

import yaml


def write_demo_project(root: Path, *, extra: dict | None = None) -> Path:
    (root / "train.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import argparse",
                "p = argparse.ArgumentParser()",
                "p.add_argument('--lr')",
                "args = p.parse_args()",
                "out = Path('checkpoints')",
                "out.mkdir(exist_ok=True)",
                "(out / f'step_{str(args.lr).replace(\".\", \"\")}.pt').write_text('ckpt')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "eval.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import argparse, os",
                "p = argparse.ArgumentParser()",
                "p.add_argument('--checkpoint_path')",
                "args = p.parse_args()",
                "assert args.checkpoint_path == os.environ['SFORGE_INPUT_CHECKPOINT']",
                "out = Path('eval')",
                "out.mkdir(exist_ok=True)",
                "(out / 'metrics.csv').write_text('metric,value\\nacc,1\\n')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cfg = {
        "project": "demo",
        "experiment": "stage_pipeline",
        "storage": {"root": str(root / "runs")},
        "environments": {
            "default": {
                "modules": [],
                "source": [],
                "env": {"DEMO_ENV": "1"},
            }
        },
        "runtime": {
            "executor": {
                "python": {"bin": "python3.11", "min_version": "3.10"},
            },
            "user": {"default": {"python": {"bin": "python3.11", "min_version": "3.10"}}},
        },
        "artifact_store": {"strategy": "copy", "verify_digest": True},
        "runs": {"type": "grid", "axes": {"train.entry.args.lr": [0.001]}},
        "stages": {
            "train": {
                "kind": "train",
                "environment": "default",
                "entry": {
                    "type": "python_script",
                    "script": "train.py",
                    "workdir": str(root),
                },
                "resources": {"nodes": 1, "gpus_per_node": 1, "cpus_per_task": 1, "constraint": "base"},
                "outputs": {
                    "checkpoint": {
                        "kind": "file",
                        "required": True,
                        "discover": {"globs": ["checkpoints/**/*.pt"], "select": "latest_step"},
                    }
                },
            },
            "eval": {
                "kind": "eval",
                "environment": "default",
                "depends_on": ["train"],
                "entry": {
                    "type": "python_script",
                    "script": "eval.py",
                    "workdir": str(root),
                },
                "inputs": {
                    "checkpoint": {
                        "source": {"kind": "upstream_output", "stage": "train", "output": "checkpoint"},
                        "expects": "path",
                        "required": True,
                        "inject": {"flag": "checkpoint_path", "env": "SFORGE_INPUT_CHECKPOINT"},
                    }
                },
                "resources": {"nodes": 1, "gpus_per_node": 1, "cpus_per_task": 1},
                "outputs": {
                    "eval_csv": {
                        "kind": "files",
                        "discover": {"globs": ["eval/**/*.csv"]},
                    }
                },
            },
        },
        "dispatch": {"max_available_gpus": 2, "overflow_policy": "serialize_groups"},
        "orchestration": {
            "controller": {
                "partition": "cpu",
                "cpus": 1,
                "mem": "2G",
                "time_limit": "01:00:00",
            },
        },
    }
    if extra:
        cfg.update(extra)
    cfg_path = root / "experiment.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return cfg_path
