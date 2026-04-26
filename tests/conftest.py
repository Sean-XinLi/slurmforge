"""Shared test fixtures and helpers.

``write_demo_project`` is exposed as a plain function for unittest-style
tests that import it directly.
"""
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
        "matrix": {"axes": {"train.entry.args.lr": [0.001]}},
        "runtime": {
            "executor": {
                "python": {"bin": "python3.11", "min_version": "3.10"},
                "bootstrap": {"steps": []},
                "env": {"DEMO_ENV": "1"},
            },
            "user": {"default": {"python": {"bin": "python3.11", "min_version": "3.10"}}},
        },
        "artifact_store": {"strategy": "copy", "verify_digest": True},
        "stages": {
            "train": {
                "kind": "train",
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
                        "discover": {"globs": ["eval/**/*.csv"], "select": "last"},
                    }
                },
            },
        },
        "dispatch": {"max_available_gpus": 2, "overflow_policy": "serialize_groups"},
        "orchestration": {
            "controller_partition": "cpu",
            "controller_cpus": 1,
            "controller_mem": "2G",
            "controller_time_limit": "01:00:00",
        },
    }
    if extra:
        cfg.update(extra)
    cfg_path = root / "experiment.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return cfg_path

