from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..errors import ConfigContractError


MAX_DEPENDENCY_LENGTH = 3500


def dependency_expression(
    job_ids: tuple[str, ...], *, dependency_type: str = "afterany"
) -> str:
    return f"{dependency_type}:{':'.join(job_ids)}"


def dependency_chunks(
    job_ids: tuple[str, ...],
    *,
    dependency_type: str = "afterany",
    max_length: int = MAX_DEPENDENCY_LENGTH,
) -> tuple[tuple[str, ...], ...]:
    chunks: list[tuple[str, ...]] = []
    current: list[str] = []
    for job_id in job_ids:
        candidate = tuple([*current, job_id])
        if current and len(
            dependency_expression(candidate, dependency_type=dependency_type)
        ) > max_length:
            chunks.append(tuple(current))
            current = [job_id]
            continue
        if (
            len(dependency_expression((job_id,), dependency_type=dependency_type))
            > max_length
        ):
            raise ConfigContractError(
                f"Scheduler job id is too long for a dependency expression: {job_id}"
            )
        current.append(job_id)
    if current:
        chunks.append(tuple(current))
    return tuple(chunks)


def dependency_sink_group_ids(batch) -> tuple[str, ...]:
    group_ids = {group.group_id for group in batch.group_plans}
    has_outgoing_dependency: set[str] = set()
    for dep in batch.budget_plan.dependencies:
        has_outgoing_dependency.update(str(item) for item in dep.from_groups if item)
    sinks = sorted(group_ids - has_outgoing_dependency)
    return tuple(sinks or sorted(group_ids))


def submit_dependent_job_with_dependency_tree(
    *,
    target_path: Path,
    dependency_job_ids: tuple[str, ...],
    client: Any,
    barrier_path_factory: Callable[[int], Path],
    dependency_type: str = "afterany",
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> tuple[str, tuple[str, ...]]:
    if not dependency_job_ids:
        raise ConfigContractError("Cannot submit a dependent job without dependencies")
    current = dependency_job_ids
    barrier_job_ids: list[str] = []
    barrier_index = 1
    while (
        len(dependency_expression(current, dependency_type=dependency_type))
        > max_dependency_length
    ):
        next_level: list[str] = []
        for chunk in dependency_chunks(
            current,
            dependency_type=dependency_type,
            max_length=max_dependency_length,
        ):
            barrier_path = barrier_path_factory(barrier_index)
            barrier_index += 1
            barrier_id = client.submit(
                barrier_path,
                dependency=dependency_expression(chunk, dependency_type=dependency_type),
            )
            barrier_job_ids.append(barrier_id)
            next_level.append(barrier_id)
        current = tuple(next_level)
    target_job_id = client.submit(
        target_path,
        dependency=dependency_expression(current, dependency_type=dependency_type),
    )
    return target_job_id, tuple(barrier_job_ids)
