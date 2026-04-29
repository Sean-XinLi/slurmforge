from __future__ import annotations

from pathlib import Path

from ..config_contract.starter_io import (
    ACCURACY_FIELD,
    ACCURACY_FILE,
    CHECKPOINT_DIR,
    CHECKPOINT_ENV,
    CHECKPOINT_FLAG,
    CHECKPOINT_GLOB,
    CHECKPOINT_SUFFIX,
)
from .markers import replace_between_markers

QUICKSTART_CONTRACT_START = "<!-- QUICKSTART_STARTER_CONTRACT_START -->"
QUICKSTART_CONTRACT_END = "<!-- QUICKSTART_STARTER_CONTRACT_END -->"


def render_quickstart_doc(current: str, *, path: Path) -> str:
    return replace_between_markers(
        current,
        QUICKSTART_CONTRACT_START,
        QUICKSTART_CONTRACT_END,
        render_quickstart_contract(),
        path=path,
    )


def render_quickstart_contract() -> str:
    return "\n".join(
        (
            "- `stages.train.entry.args` keys become CLI flags exactly as written; SlurmForge only prepends `--` when the key has no dash prefix.",
            f"- Train writes a `{CHECKPOINT_SUFFIX}` checkpoint under `{CHECKPOINT_DIR}/`; the starter YAML discovers it with `{CHECKPOINT_GLOB}`.",
            f"- Eval receives that checkpoint as `--{CHECKPOINT_FLAG}` and `{CHECKPOINT_ENV}`.",
            f"- Eval writes `{ACCURACY_FILE}` with a numeric `{ACCURACY_FIELD}` field.",
        )
    )
