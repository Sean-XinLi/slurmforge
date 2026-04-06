from __future__ import annotations

OOM_PATTERNS = (
    "cuda out of memory",
    "out of memory",
    "oom-kill",
    "killed process",
    "std::bad_alloc",
    "memoryerror",
    "cublas_status_alloc_failed",
    "failed to allocate memory",
)

PREEMPTED_PATTERNS = (
    "due to preemption",
    "preempted",
    "has been requeued",
    "job requeued",
)

NODE_FAILURE_PATTERNS = (
    "node_fail",
    "node failure",
    "batch job aborted: node failure",
    "slurmd: error",
    "unable to contact slurm controller",
    "communication connection failure",
)
