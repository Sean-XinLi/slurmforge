from __future__ import annotations

from ..errors import ConfigContractError
from ..plans.stage import StageBatchPlan


def dependency_for(group_id: str, batch: StageBatchPlan, group_job_ids: dict[str, str]) -> str | None:
    deps = {item.to_group: item for item in batch.budget_plan.dependencies}
    dep = deps.get(group_id)
    if dep is None:
        return None
    missing = [str(item) for item in dep.from_groups if item and str(item) not in group_job_ids]
    if missing:
        raise ConfigContractError(
            f"Cannot submit `{group_id}` before dependency groups have scheduler ids: {', '.join(missing)}"
        )
    from_job_ids = [group_job_ids[str(item)] for item in dep.from_groups if item]
    return f"{dep.type}:{':'.join(from_job_ids)}"
