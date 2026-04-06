from __future__ import annotations

from pathlib import Path


def train_outputs_manifest_path_for_result_dir(result_dir: Path) -> Path:
    return result_dir / "meta" / "train_outputs.json"


def train_outputs_env_path_for_result_dir(result_dir: Path) -> Path:
    return result_dir / "meta" / "train_outputs.env"
