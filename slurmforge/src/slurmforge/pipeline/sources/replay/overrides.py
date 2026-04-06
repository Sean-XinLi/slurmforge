from __future__ import annotations

import copy
from typing import Any, Sequence

from ....sweep import deep_set, parse_override


def parse_cli_overrides(cli_overrides: Sequence[str]) -> tuple[tuple[str, Any], ...]:
    return tuple(parse_override(item) for item in cli_overrides)


def apply_cli_overrides(
    replay_cfg: dict[str, Any],
    *,
    parsed_overrides: Sequence[tuple[str, Any]],
) -> dict[str, Any]:
    run_cfg = copy.deepcopy(replay_cfg)
    for key, value in parsed_overrides:
        deep_set(run_cfg, key, value)
    return run_cfg
