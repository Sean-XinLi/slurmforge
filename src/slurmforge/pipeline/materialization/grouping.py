"""Re-export of planning-layer grouping primitives.

The canonical implementation lives in ``pipeline/planning/dispatch_grouping.py``
so that planning, validate, dry-run and materialization all consume the
exact same grouping logic.  This module exists for import-path stability.
"""
from __future__ import annotations

from ..planning.dispatch_grouping import (
    RUNTIME_ENV_GROUPING_KEYS,
    SLURM_RESOURCE_GROUPING_KEYS,
    array_group_signature,
    array_grouping_fields,
    describe_array_group_reason,
    ensure_cluster_renderable,
    resource_bucket_from_cluster,
    resource_request_from_cluster,
    summarize_resource_buckets,
)

__all__ = [
    "RUNTIME_ENV_GROUPING_KEYS",
    "SLURM_RESOURCE_GROUPING_KEYS",
    "array_group_signature",
    "array_grouping_fields",
    "describe_array_group_reason",
    "ensure_cluster_renderable",
    "resource_bucket_from_cluster",
    "resource_request_from_cluster",
    "summarize_resource_buckets",
]
