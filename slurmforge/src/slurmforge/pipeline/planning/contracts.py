from __future__ import annotations

from .codecs import (
    parse_allocation_request,
    parse_execution_topology,
    parse_plan_diagnostic,
    parse_resource_estimate,
    parse_stage_capabilities,
    parse_stage_execution_plan,
    serialize_allocation_request,
    serialize_execution_topology,
    serialize_plan_diagnostic,
    serialize_resource_estimate,
    serialize_stage_capabilities,
    serialize_stage_execution_plan,
)
from .models import (
    AllocationRequest,
    ExecutionTopology,
    PlanDiagnostic,
    ResourceEstimate,
    StageCapabilities,
    StageExecutionPlan,
)

ensure_allocation_request = parse_allocation_request
ensure_execution_topology = parse_execution_topology
ensure_plan_diagnostic = parse_plan_diagnostic
ensure_resource_estimate = parse_resource_estimate
ensure_stage_capabilities = parse_stage_capabilities
ensure_stage_execution_plan = parse_stage_execution_plan

__all__ = [
    "AllocationRequest",
    "ExecutionTopology",
    "PlanDiagnostic",
    "ResourceEstimate",
    "StageCapabilities",
    "StageExecutionPlan",
    "ensure_allocation_request",
    "ensure_execution_topology",
    "ensure_plan_diagnostic",
    "ensure_resource_estimate",
    "ensure_stage_capabilities",
    "ensure_stage_execution_plan",
    "parse_allocation_request",
    "parse_execution_topology",
    "parse_plan_diagnostic",
    "parse_resource_estimate",
    "parse_stage_capabilities",
    "parse_stage_execution_plan",
    "serialize_allocation_request",
    "serialize_execution_topology",
    "serialize_plan_diagnostic",
    "serialize_resource_estimate",
    "serialize_stage_capabilities",
    "serialize_stage_execution_plan",
]
