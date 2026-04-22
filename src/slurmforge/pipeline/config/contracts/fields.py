"""Field-level contract registry.

Single source of truth for every config field's semantics:

    - lifecycle ("batch", "run", "meta")
    - source    ("authoring_only", "replay_only", "both")
    - sweep_allowed
    - batch_resolver ("unique" | "first_wins" | None)

Any time a new config field is added to the schema, it MUST be declared
here too.  A meta-test (``tests/test_contracts.py``) walks the schema and
asserts every leaf path resolves to a contract; unregistered fields fail.

Path matching
-------------
- An entry's ``path`` is either an exact dotted key (``resources.max_available_gpus``)
  or a wildcard like ``output.*`` / ``run.args.*`` meaning "this prefix and
  anything beneath it".
- When multiple entries match a query, the most specific one wins
  (longest literal prefix).
- Source filtering narrows the candidate set: ``source="authoring"``
  excludes ``replay_only`` entries (and vice versa).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Lifecycle = Literal["batch", "run", "meta"]
Source = Literal["authoring_only", "replay_only", "both"]
BatchResolverKind = Literal["unique", "first_wins"]


@dataclass(frozen=True)
class FieldContract:
    path: str
    lifecycle: Lifecycle
    source: Source
    sweep_allowed: bool
    batch_resolver: BatchResolverKind | None = None
    description: str = ""


# ---------------------------------------------------------------------------
# The registry.
#
# Order has no semantic effect; ``contract_for_path`` picks the most specific
# entry (longest literal prefix).  Grouped by lifecycle/source for reading.
# ---------------------------------------------------------------------------

_REGISTRY: tuple[FieldContract, ...] = (
    # ---- Batch-scoped identity / bookkeeping fields -----------------------
    FieldContract(
        path="project",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description="Batch identity component.",
    ),
    FieldContract(
        path="experiment_name",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description="Batch identity component.",
    ),
    # ``output`` is split into three explicit entries so ``output.batch_name``
    # can document its replay-specific "derived_or_override" semantics without
    # polluting the other two, which carry normal batch-scoped first-wins
    # behavior.
    FieldContract(
        path="output.base_output_dir",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description="Batch output root directory.",
    ),
    FieldContract(
        path="output.dependencies.*",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description="External Slurm dependencies applied to every array job in this batch.",
    ),
    FieldContract(
        path="output.batch_name",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description=(
            "Batch name. In authoring it comes from config or a timestamp default. "
            "In replay/rerun, the original batch_name stored on each source run is NOT "
            "enforced for consistency: every new batch uses ``default_batch_name`` unless "
            "the user explicitly passes ``--set output.batch_name=...``. See "
            "``resolve_replay_batch_identity``."
        ),
    ),
    FieldContract(
        path="notify.*",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description="Batch completion notifications.",
    ),
    FieldContract(
        path="storage.*",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="first_wins",
        description="Batch storage backend and exports.",
    ),

    # ---- Batch-scoped GPU budget & dispatch policy ------------------------
    FieldContract(
        path="resources.max_available_gpus",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="unique",
        description="Total GPU budget for the whole batch.",
    ),
    FieldContract(
        path="dispatch.group_overflow_policy",
        lifecycle="batch",
        source="both",
        sweep_allowed=False,
        batch_resolver="unique",
        description="How array groups share max_available_gpus.",
    ),

    # ---- Run-scoped planning / topology fields ----------------------------
    FieldContract(path="model.*",    lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(path="run.*",      lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(path="launcher.*", lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(path="cluster.*",  lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(path="env.*",      lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(
        path="resources.auto_gpu",
        lifecycle="run", source="both", sweep_allowed=True,
        description="Run-scoped estimator switch.",
    ),
    FieldContract(
        path="resources.gpu_estimator",
        lifecycle="run", source="both", sweep_allowed=True,
    ),
    FieldContract(
        path="resources.target_mem_per_gpu_gb",
        lifecycle="run", source="both", sweep_allowed=True,
    ),
    FieldContract(
        path="resources.safety_factor",
        lifecycle="run", source="both", sweep_allowed=True,
    ),
    FieldContract(
        path="resources.min_gpus_per_job",
        lifecycle="run", source="both", sweep_allowed=True,
    ),
    FieldContract(
        path="resources.max_gpus_per_job",
        lifecycle="run", source="both", sweep_allowed=True,
        description="Per-run GPU cap. Independent of resources.max_available_gpus.",
    ),
    FieldContract(path="artifacts.*",  lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(path="eval.*",       lifecycle="run", source="both", sweep_allowed=True),
    FieldContract(path="validation.*", lifecycle="run", source="both", sweep_allowed=True),

    # ---- Meta / source-only fields ----------------------------------------
    FieldContract(
        path="model_registry.*",
        lifecycle="meta",
        source="authoring_only",
        sweep_allowed=False,
        description="Authoring-side model registry lookup.",
    ),
    FieldContract(
        path="resolved_model_catalog.*",
        lifecycle="meta",
        source="replay_only",
        sweep_allowed=False,
        description=(
            "Per-run replay catalog.  Runs from different historical batches "
            "may legitimately carry different resolved catalogs, so this field "
            "does NOT participate in any batch-level unique resolver."
        ),
    ),
    FieldContract(
        path="sweep.*",
        lifecycle="meta",
        source="authoring_only",
        sweep_allowed=False,
        description="Sweep directive itself; consumed before run normalization.",
    ),
)


# ---------------------------------------------------------------------------
# Lookup primitives.
# ---------------------------------------------------------------------------


def _pattern_prefix(pattern: str) -> str:
    """Return the literal prefix of a registry pattern.

    - ``resources.max_available_gpus`` → ``resources.max_available_gpus``
    - ``output.*`` → ``output``
    """
    if pattern.endswith(".*"):
        return pattern[:-2]
    return pattern


def _pattern_matches(pattern: str, path: str) -> bool:
    """Does a query ``path`` fall under the registry ``pattern``?

    ``run.*`` matches ``run``, ``run.args``, ``run.args.lr``, and ``run.args.*``
    (the last form is what ``walk_schema_leaf_paths`` emits for a
    ``DynamicMapping`` leaf).
    """
    if pattern == path:
        return True
    if pattern.endswith(".*"):
        prefix = pattern[:-2]
        return path == prefix or path.startswith(prefix + ".")
    return False


def _source_allows(contract: FieldContract, query_source: str | None) -> bool:
    if query_source is None:
        return True
    if contract.source == "both":
        return True
    if contract.source == "authoring_only":
        return query_source == "authoring"
    if contract.source == "replay_only":
        return query_source == "replay"
    return False


def contract_for_path(
    path: str,
    *,
    source: Literal["authoring", "replay"] | None = None,
) -> FieldContract | None:
    """Resolve a config path to its contract.

    The most specific matching registry entry wins (longest literal prefix).
    Returns ``None`` when no entry matches, which means the field is
    unregistered — the caller (or meta-test) should treat that as an error.
    """
    best: FieldContract | None = None
    best_specificity = -1
    for contract in _REGISTRY:
        if not _pattern_matches(contract.path, path):
            continue
        if not _source_allows(contract, source):
            continue
        specificity = len(_pattern_prefix(contract.path))
        if specificity > best_specificity:
            best = contract
            best_specificity = specificity
    return best


def sweep_allowed(path: str) -> bool:
    contract = contract_for_path(path, source="authoring")
    if contract is None:
        # Unregistered authoring path — refuse conservatively.  Meta-test
        # will catch the real gap before this matters in production.
        return False
    return contract.sweep_allowed


def is_batch_scoped(path: str) -> bool:
    contract = contract_for_path(path)
    return contract is not None and contract.lifecycle == "batch"


def batch_resolver_fields(
    kind: BatchResolverKind | None = None,
) -> tuple[FieldContract, ...]:
    """Return all batch-lifecycle contracts that participate in a resolver.

    ``kind=None`` returns every batch-level contract that has a resolver set
    (either ``"unique"`` or ``"first_wins"``).  Pass a specific kind to
    narrow.
    """
    out = []
    for contract in _REGISTRY:
        if contract.batch_resolver is None:
            continue
        if kind is not None and contract.batch_resolver != kind:
            continue
        out.append(contract)
    return tuple(out)


def all_contracts() -> tuple[FieldContract, ...]:
    return _REGISTRY
