from __future__ import annotations

import copy

from ....sources.replay import resolve_replay_batch_identity
from ...state import (
    BatchFirstWinsState,
    CompileState,
    MaterializedSourceBundle,
    ReplayMaterializedState,
)
from ....planning.contracts import PlanDiagnostic


def initialize_replay_compile_state(
    materialized: MaterializedSourceBundle,
    *,
    include_planning: bool,
) -> CompileState:
    return CompileState(batch_diagnostics=materialized.batch_diagnostics)


# ---------------------------------------------------------------------------
# Batch-level diagnostics for replay/rerun.
#
# Replay reconstructs every run independently from its stored YAML, so
# different selected runs can disagree on batch-scoped fields.  Instead of
# raising ``InternalCompilerError`` (which is meant for "this can't happen"
# framework bugs), we surface user-actionable ``PlanDiagnostic`` entries and
# let the compile report mark the batch as failed.  The user can then
# resolve the divergence with ``--set <path>=<value>``.
# ---------------------------------------------------------------------------


def _batch_mismatch_diagnostic(
    *,
    code: str,
    message: str,
    field_path: str,
) -> PlanDiagnostic:
    # Category is "config", not "resource": these are batch-contract
    # violations (the user's replay selection picked runs whose batch-scoped
    # config disagrees), not GPU / memory / allocation problems.  Keeping
    # the category accurate matters for downstream diagnostic filtering.
    return PlanDiagnostic(
        severity="error",
        category="config",
        code=code,
        message=message,
        stage="batch",
        field_path=field_path,
    )


def _identity_mismatch_diagnostic(existing, candidate) -> PlanDiagnostic:
    return _batch_mismatch_diagnostic(
        code="replay_identity_mismatch",
        message=(
            "selected replay runs do not resolve to a single batch identity "
            f"({existing.project}/{existing.experiment_name}/{existing.batch_name} "
            f"vs {candidate.project}/{candidate.experiment_name}/{candidate.batch_name}). "
            "Use --set project=..., --set experiment_name=..., "
            "--set output.base_output_dir=..., or --set output.batch_name=... "
            "to create a new batch."
        ),
        field_path="output.batch_name",
    )


def _notify_mismatch_diagnostic() -> PlanDiagnostic:
    return _batch_mismatch_diagnostic(
        code="replay_notify_mismatch",
        message=(
            "selected replay runs have multiple notify configurations. "
            "Use --set notify.enabled=..., --set notify.email=..., or "
            "--set notify.when=... to unify."
        ),
        field_path="notify",
    )


def _submit_dependencies_mismatch_diagnostic() -> PlanDiagnostic:
    return _batch_mismatch_diagnostic(
        code="replay_submit_dependencies_mismatch",
        message=(
            "selected replay runs carry different output.dependencies. "
            "Use --set output.dependencies=... to unify or pick runs from one batch."
        ),
        field_path="output.dependencies",
    )


def _storage_mismatch_diagnostic() -> PlanDiagnostic:
    return _batch_mismatch_diagnostic(
        code="replay_storage_mismatch",
        message=(
            "selected replay runs have different storage configurations. "
            "Use --set storage.backend.engine=... to unify."
        ),
        field_path="storage",
    )


def accept_replay_spec(
    state: CompileState,
    *,
    spec,
    materialized: MaterializedSourceBundle,
    source_input,
) -> CompileState:
    from dataclasses import replace

    context = materialized.context
    assert isinstance(context, ReplayMaterializedState)
    candidate_identity = resolve_replay_batch_identity(
        spec,
        project_root=context.project_root,
        default_batch_name=context.default_batch_name,
        parsed_overrides=context.parsed_overrides,
    )

    new_diagnostics: list[PlanDiagnostic] = []

    if state.first_wins is None:
        # First spec seeds the batch-scoped "first_wins" fields.  Candidate
        # accumulation for the "unique"-resolved scalars (max_available_gpus
        # / dispatch policy) happens unconditionally below.
        base = CompileState(
            first_wins=BatchFirstWinsState(
                identity=candidate_identity,
                notify_cfg=spec.notify,
                submit_dependencies=copy.deepcopy(spec.output.dependencies),
                storage_config=spec.storage,
            ),
            batch_diagnostics=state.batch_diagnostics,
        )
    else:
        # Subsequent specs must agree on every first_wins field.  We record
        # all mismatches rather than short-circuiting on the first one so the
        # user sees the full picture in one validate cycle.  The set of
        # fields checked here is locked to the registry's "first_wins"
        # contracts via ``tests/test_contracts.py``; adding a new first_wins
        # contract without extending these checks triggers a test failure.
        first_wins = state.first_wins
        if candidate_identity != first_wins.identity:
            new_diagnostics.append(_identity_mismatch_diagnostic(first_wins.identity, candidate_identity))
        if spec.notify != first_wins.notify_cfg:
            new_diagnostics.append(_notify_mismatch_diagnostic())
        if spec.output.dependencies != first_wins.submit_dependencies:
            new_diagnostics.append(_submit_dependencies_mismatch_diagnostic())
        if spec.storage != first_wins.storage_config:
            new_diagnostics.append(_storage_mismatch_diagnostic())
        base = state

    # Unique-resolver batch scalars: accumulate candidates here; the final
    # consistency check and the single winning value happen in
    # ``build_materialized_report``.
    base = replace(
        base,
        max_available_gpus_candidates=base.max_available_gpus_candidates + (int(spec.resources.max_available_gpus),),
        dispatch_policy_candidates=base.dispatch_policy_candidates + (str(spec.dispatch.group_overflow_policy),),
        batch_diagnostics=base.batch_diagnostics + tuple(new_diagnostics),
    )
    return base
