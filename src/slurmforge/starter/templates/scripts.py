from __future__ import annotations

from importlib.resources import files
from pathlib import Path

from ...defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_EVAL_SCRIPT,
    DEFAULT_TRAIN_SCRIPT,
)
from ..models import FilePayload, InitRequest


def train_script(_request: InitRequest) -> FilePayload:
    return FilePayload(
        relative_path=Path(DEFAULT_TRAIN_SCRIPT),
        role="script",
        content=_asset_text("train.py"),
    )


def eval_script(_request: InitRequest) -> FilePayload:
    return FilePayload(
        relative_path=Path(DEFAULT_EVAL_SCRIPT),
        role="script",
        content=_asset_text("eval.py"),
    )


def checkpoint_file(_request: InitRequest) -> FilePayload:
    return FilePayload(
        relative_path=Path(DEFAULT_CHECKPOINT_PATH),
        role="sample-input",
        content="sample checkpoint for sforge init\n",
    )


def _asset_text(name: str) -> str:
    return (
        files("slurmforge.starter.templates.assets")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )
