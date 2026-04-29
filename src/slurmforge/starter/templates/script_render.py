from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
import re

from ...config_contract.starter_io import (
    ACCURACY_FIELD,
    ACCURACY_FILE,
    ACCURACY_JSON_PATH,
    CHECKPOINT_DIR,
    CHECKPOINT_ENV,
    CHECKPOINT_FLAG,
    CHECKPOINT_GLOB,
    CHECKPOINT_SUFFIX,
    EVAL_SPLIT_DEFAULT,
)
from ..errors import StarterTemplateError

PLACEHOLDER_RE = re.compile(r"__SFORGE_[A-Z0-9_]+__")


@dataclass(frozen=True)
class ScriptTemplateContext:
    checkpoint_dir: str = CHECKPOINT_DIR
    checkpoint_env: str = CHECKPOINT_ENV
    checkpoint_flag: str = CHECKPOINT_FLAG
    checkpoint_glob: str = CHECKPOINT_GLOB
    checkpoint_suffix: str = CHECKPOINT_SUFFIX
    accuracy_field: str = ACCURACY_FIELD
    accuracy_file: str = ACCURACY_FILE
    accuracy_json_path: str = ACCURACY_JSON_PATH
    eval_split_default: str = EVAL_SPLIT_DEFAULT


DEFAULT_SCRIPT_TEMPLATE_CONTEXT = ScriptTemplateContext()


def render_train_asset(
    context: ScriptTemplateContext = DEFAULT_SCRIPT_TEMPLATE_CONTEXT,
) -> str:
    return _render_asset(
        "train.py",
        {
            "__SFORGE_CHECKPOINT_DIR__": context.checkpoint_dir,
            "__SFORGE_CHECKPOINT_GLOB__": context.checkpoint_glob,
            "__SFORGE_CHECKPOINT_SUFFIX__": context.checkpoint_suffix,
        },
    )


def render_eval_asset(
    context: ScriptTemplateContext = DEFAULT_SCRIPT_TEMPLATE_CONTEXT,
) -> str:
    return _render_asset(
        "eval.py",
        {
            "__SFORGE_CHECKPOINT_FLAG__": context.checkpoint_flag,
            "__SFORGE_CHECKPOINT_ENV__": context.checkpoint_env,
            "__SFORGE_ACCURACY_FILE__": context.accuracy_file,
            "__SFORGE_ACCURACY_FIELD__": context.accuracy_field,
            "__SFORGE_ACCURACY_JSON_PATH__": context.accuracy_json_path,
            "__SFORGE_EVAL_SPLIT_DEFAULT__": context.eval_split_default,
        },
    )


def _render_asset(name: str, replacements: dict[str, str]) -> str:
    content = _asset_text(name)
    _validate_placeholders(name, content, replacements)
    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)
    remaining = sorted(set(PLACEHOLDER_RE.findall(content)))
    if remaining:
        raise StarterTemplateError(
            f"{name} has unreplaced placeholders: {', '.join(remaining)}"
        )
    return content


def _validate_placeholders(
    name: str, content: str, replacements: dict[str, str]
) -> None:
    placeholders = set(PLACEHOLDER_RE.findall(content))
    replacement_keys = set(replacements)
    missing = sorted(placeholders - replacement_keys)
    unused = sorted(replacement_keys - placeholders)
    if missing:
        raise StarterTemplateError(
            f"{name} has undeclared placeholders: {', '.join(missing)}"
        )
    if unused:
        raise StarterTemplateError(
            f"{name} declares unused placeholders: {', '.join(unused)}"
        )


def _asset_text(name: str) -> str:
    return (
        files("slurmforge.starter.templates.assets")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )
