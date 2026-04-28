from __future__ import annotations

from ..planner import build_resource_estimate as _build_resource_estimate
from ..planner import render_resource_estimate as _render_resource_estimate


def build_resource_estimate_for_plan(plan):
    return _build_resource_estimate(plan)


def render_resource_estimate_for_plan(plan) -> list[str]:
    return _render_resource_estimate(_build_resource_estimate(plan))
