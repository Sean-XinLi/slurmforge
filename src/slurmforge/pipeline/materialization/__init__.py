from __future__ import annotations

from .api import materialize_batch
from .context import MaterializationResult
from .grouping import (
    array_group_signature,
    array_grouping_fields,
    describe_array_group_reason,
    ensure_cluster_renderable,
    resource_bucket_from_cluster,
    resource_request_from_cluster,
    summarize_resource_buckets,
)
from .reporting import print_dry_run, print_dry_run_batch
from .shell_builder import build_shell_script

__all__ = [
    "MaterializationResult",
    "array_group_signature",
    "array_grouping_fields",
    "build_shell_script",
    "describe_array_group_reason",
    "ensure_cluster_renderable",
    "materialize_batch",
    "print_dry_run",
    "print_dry_run_batch",
    "resource_bucket_from_cluster",
    "resource_request_from_cluster",
    "summarize_resource_buckets",
]
