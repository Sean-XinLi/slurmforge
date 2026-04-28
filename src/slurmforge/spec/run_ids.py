from __future__ import annotations

import re
from typing import Any

from ..errors import ConfigContractError
from ..io import content_digest


def run_id_for(index: int, run_overrides: dict[str, Any], spec_digest: str) -> str:
    seed = {
        "index": index,
        "run_overrides": run_overrides,
        "spec_snapshot_digest": spec_digest,
    }
    digest = content_digest(seed, prefix=10)
    return f"run_{index:04d}_{digest}"


def matrix_run_id_for(
    case_name: str,
    combo_index: int,
    run_overrides: dict[str, Any],
    spec_digest: str,
) -> str:
    seed = {
        "case_name": case_name,
        "combo_index": combo_index,
        "run_overrides": run_overrides,
        "spec_snapshot_digest": spec_digest,
    }
    digest = content_digest(seed, prefix=8)
    return f"{validate_case_run_id(case_name)}.grid_{combo_index:04d}_{digest}"


def validate_case_run_id(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise ConfigContractError(
            "`runs.cases[].name` may only contain letters, numbers, underscores, dots, and dashes"
        )
    return name
