from __future__ import annotations

from pathlib import Path

from ...config_contract.defaults import (
    DEFAULT_CHECKPOINT_PATH,
    DEFAULT_EVAL_SCRIPT,
    DEFAULT_TRAIN_SCRIPT,
)
from ..models import FilePayload, InitRequest
from .script_render import render_eval_asset, render_train_asset


def train_script(_request: InitRequest) -> FilePayload:
    return FilePayload(
        relative_path=Path(DEFAULT_TRAIN_SCRIPT),
        role="script",
        content=render_train_asset(),
    )


def eval_script(_request: InitRequest) -> FilePayload:
    return FilePayload(
        relative_path=Path(DEFAULT_EVAL_SCRIPT),
        role="script",
        content=render_eval_asset(),
    )


def checkpoint_file(_request: InitRequest) -> FilePayload:
    return FilePayload(
        relative_path=Path(DEFAULT_CHECKPOINT_PATH),
        role="sample-input",
        content="sample checkpoint for sforge init\n",
    )
