from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from slurmforge.starter import InitRequest, create_starter_project


DEFAULT_PROFILE = "stage_batch_default"
DEFAULT_REPLACE_SECTIONS = ("runs",)


def write_demo_project(
    root: Path,
    *,
    profile: str = DEFAULT_PROFILE,
    extra: dict | None = None,
    replace_sections: tuple[str, ...] = DEFAULT_REPLACE_SECTIONS,
) -> Path:
    cfg_path = root / "experiment.yaml"
    create_starter_project(InitRequest(template="train-eval", output=cfg_path, force=True))
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    if profile == "stage_batch_default":
        _apply_stage_batch_default_profile(cfg, root)
    else:
        raise ValueError(f"Unknown demo project profile: {profile}")
    if extra:
        cfg = _deep_merge(cfg, extra, replace_sections=frozenset(replace_sections))
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    return cfg_path


def _apply_stage_batch_default_profile(cfg: dict[str, Any], root: Path) -> None:
    cfg["experiment"] = "stage_pipeline"
    cfg["storage"] = {"root": str(root / "runs")}
    cfg["environments"]["default"]["env"] = {"DEMO_ENV": "1"}
    cfg["runs"] = {"type": "grid", "axes": {"train.entry.args.lr": [0.001]}}
    cfg["stages"]["train"]["entry"]["workdir"] = str(root)
    cfg["stages"]["train"]["entry"]["args"] = {"lr": 0.001}
    cfg["stages"]["eval"]["entry"]["workdir"] = str(root)
    cfg["stages"]["eval"]["entry"]["args"] = {}
    cfg["stages"]["eval"]["outputs"] = {
        "eval_csv": {
            "kind": "files",
            "discover": {"globs": ["eval/**/*.csv"]},
        }
    }
    cfg["stages"]["train"]["resources"].update(
        {
            "nodes": 1,
            "gpus_per_node": 1,
            "cpus_per_task": 1,
            "constraint": "base",
        }
    )
    cfg["stages"]["eval"]["resources"].update(
        {
            "nodes": 1,
            "gpus_per_node": 1,
            "cpus_per_task": 1,
        }
    )
    cfg["orchestration"]["controller"].update(
        {
            "partition": "cpu",
            "cpus": 1,
            "mem": "2G",
            "time_limit": "01:00:00",
            "environment": "",
        }
    )


def _deep_merge(
    base: dict[str, Any],
    override: dict[str, Any],
    *,
    replace_sections: frozenset[str] = frozenset(),
    path: tuple[str, ...] = (),
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        section_path = (*path, str(key))
        dotted_path = ".".join(section_path)
        existing = merged.get(key)
        if dotted_path not in replace_sections and isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge(
                existing,
                value,
                replace_sections=replace_sections,
                path=section_path,
            )
        else:
            merged[key] = value
    return merged
