from __future__ import annotations


def master_fallback(job_states: dict[str, object], job_id: str, *, group_size: int):
    master = job_states.get(job_id)
    if master is None or group_size <= 1:
        return master
    if master.is_terminal and not master.is_success:
        return master
    return None
