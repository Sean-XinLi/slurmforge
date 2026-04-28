from __future__ import annotations

from .models import StageStatusRecord


def state_matches(status: StageStatusRecord | None, query: str) -> bool:
    if query in {"", "all", "*"}:
        return True
    if status is None:
        return query in {"missing", "state=missing"}
    if "=" in query:
        key, value = query.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key == "state":
            return status.state == value
        if key == "failure_class":
            return status.failure_class == value
        if key == "stage":
            return status.stage_name == value
        if key == "run_id":
            return status.run_id == value
        return False
    if query == "failed":
        return status.state == "failed"
    if query == "non_success":
        return status.state != "success"
    return status.state == query or status.failure_class == query
