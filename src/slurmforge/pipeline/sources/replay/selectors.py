from __future__ import annotations

from typing import Any, Sequence

from ....errors import ConfigContractError


def normalize_run_ids(run_ids: Sequence[str]) -> tuple[str, ...]:
    normalized = []
    for raw in run_ids:
        text = str(raw or "").strip()
        if not text:
            raise ConfigContractError("replay --run_id values must be non-empty strings")
        normalized.append(text)
    return tuple(normalized)


def normalize_run_indices(run_indices: Sequence[int]) -> tuple[int, ...]:
    normalized = []
    for raw in run_indices:
        value = int(raw)
        if value < 1:
            raise ConfigContractError("replay --run_index values must be >= 1")
        normalized.append(value)
    return tuple(normalized)


def select_batch_plans(
    plans: Sequence[Any],
    *,
    run_ids: Sequence[str],
    run_indices: Sequence[int],
) -> list[Any]:
    selected_ids = set(normalize_run_ids(run_ids))
    selected_indices = set(normalize_run_indices(run_indices))

    available_ids = {plan.run_id for plan in plans}
    missing_ids = sorted(selected_ids - available_ids)
    if missing_ids:
        raise ConfigContractError(f"Replay batch source is missing run_id values: {missing_ids}")

    available_indices = {plan.run_index for plan in plans}
    missing_indices = sorted(selected_indices - available_indices)
    if missing_indices:
        raise ConfigContractError(f"Replay batch source is missing run_index values: {missing_indices}")

    selected: list[Any] = []
    for plan in plans:
        if selected_ids and plan.run_id not in selected_ids:
            continue
        if selected_indices and plan.run_index not in selected_indices:
            continue
        selected.append(plan)

    if not selected:
        raise ConfigContractError("Replay batch selection produced no runs")
    return selected
