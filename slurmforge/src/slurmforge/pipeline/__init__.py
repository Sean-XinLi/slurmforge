"""
Pipeline domain packages for slurmforge.

This package is a namespace — subpackages are imported directly via their
fully-qualified paths. Each subpackage owns its public API through its own
``__init__.py``.

Subpackages
-----------
Compilation and orchestration:
    compiler        End-to-end compile orchestration (collect → materialize → plan)
    sources         Source input collection for authoring and replay variants
    planning        Run plan assembly, fingerprinting, and validation
    materialization Sbatch script generation, batch layout, and record writing

Configuration:
    config          YAML parsing, assembly, validation, and runtime config models
    launch          Launch command building and strategy selection

Records and state:
    records         Persistent run plan / snapshot serialization and I/O
    status          Execution lifecycle tracking and failure classification
    checkpoints     Checkpoint file discovery, selection, and state management
    train_outputs   Training output manifest building and environment contract

Shared utilities:
    utils           Internal helpers (deep_merge, schema utilities)
"""
from __future__ import annotations
