"""Tests for the field-contract registry.

Covers the spec's checklist:

Meta-test:
  - every schema leaf path resolves to a contract (no silent drift).

Sweep validation (via contracts):
   1. sweep model_registry.registry_file rejected
   2. sweep model_registry.extra_models rejected
   3. sweep output.base_output_dir rejected
   4. sweep storage.backend.engine rejected
   5. sweep resources.max_available_gpus rejected
   6. sweep resources.max_gpus_per_job allowed
   7. sweep validation.cli_args allowed
   8. sweep cluster.gpus_per_node allowed
   9. sweep env.extra_env.MY_VAR allowed

Replay batch-consistency (user-facing diagnostics, not InternalCompilerError):
  10. replay runs disagreeing on resources.max_available_gpus → batch diagnostic
  11. replay --set resources.max_available_gpus=... makes it succeed
  12. replay runs disagreeing on dispatch.group_overflow_policy → batch diagnostic
  13. replay --set dispatch.group_overflow_policy=... makes it succeed
  14. replay identity mismatch → user-readable diagnostic, not InternalCompilerError
  15. retry/rerun shares the same diagnostic surface (exercised via the shared
      ``accept_replay_spec`` path).
  16. resolved_model_catalog.* divergence does NOT trigger the batch resolver.
"""
from __future__ import annotations

import unittest
from pathlib import Path

from slurmforge.errors import ConfigContractError
from slurmforge.pipeline.compiler.flows.replay.identity import accept_replay_spec
from slurmforge.pipeline.compiler.reports.builders import build_materialized_report
from slurmforge.pipeline.compiler.state import (
    CompileState,
    MaterializedSourceBundle,
    ReplayMaterializedState,
)
from slurmforge.pipeline.compiler.reports.models import SourceCollectionReport
from slurmforge.pipeline.compiler.requests import AuthoringSourceRequest
from slurmforge.pipeline.config.contracts import (
    all_contracts,
    batch_resolver_fields,
    contract_for_path,
    is_batch_scoped,
    sweep_allowed,
    walk_schema_leaf_paths,
)
from slurmforge.pipeline.config.validation.definitions import AUTHORING_SCHEMA, REPLAY_SCHEMA
from slurmforge.pipeline.config.validation.sweep_rules import validate_batch_scoped_sweep_paths
from slurmforge.pipeline.sources.models import SourceInputBatch
from slurmforge.sweep.models import SweepCaseSpec, SweepSpec


# ---------------------------------------------------------------------------
# Meta-test: the registry must cover every schema leaf.
# ---------------------------------------------------------------------------


class FieldContractCoverageTests(unittest.TestCase):
    """New config fields must come with an explicit contract.

    This test is load-bearing: if it fails, it means a schema field was
    added without declaring lifecycle/source/sweep_allowed, which in turn
    means sweep validation and replay consistency checks would silently
    skip that field.
    """

    def _collect_missing(self, schema, *, source: str) -> list[str]:
        missing: list[str] = []
        for path in walk_schema_leaf_paths(schema):
            if contract_for_path(path, source=source) is None:
                missing.append(path)
        return missing

    def test_every_authoring_schema_leaf_has_contract(self) -> None:
        missing = self._collect_missing(AUTHORING_SCHEMA, source="authoring")
        self.assertEqual(
            missing, [],
            f"authoring schema fields without contract: {missing}. "
            "Add a FieldContract entry in pipeline/config/contracts/fields.py.",
        )

    def test_every_replay_schema_leaf_has_contract(self) -> None:
        missing = self._collect_missing(REPLAY_SCHEMA, source="replay")
        self.assertEqual(
            missing, [],
            f"replay schema fields without contract: {missing}. "
            "Add a FieldContract entry in pipeline/config/contracts/fields.py.",
        )


class ContractLookupTests(unittest.TestCase):
    def test_exact_entry_wins_over_prefix_entry(self) -> None:
        # ``resources.max_available_gpus`` is registered as an exact batch
        # field, even though ``resources.*`` style patterns could in principle
        # also match.  The registry picks the most specific entry.
        contract = contract_for_path("resources.max_available_gpus")
        self.assertIsNotNone(contract)
        assert contract is not None
        self.assertEqual(contract.lifecycle, "batch")
        self.assertEqual(contract.batch_resolver, "unique")

    def test_wildcard_entry_matches_nested_paths(self) -> None:
        contract = contract_for_path("output.base_output_dir")
        self.assertIsNotNone(contract)
        assert contract is not None
        self.assertEqual(contract.lifecycle, "batch")

    def test_run_prefix_covers_dynamic_nested_paths(self) -> None:
        # Dynamic maps walk to ``run.args.*``; registry entry ``run.*`` matches.
        self.assertTrue(sweep_allowed("run.args.lr"))
        self.assertTrue(sweep_allowed("run.args.batch_size"))

    def test_authoring_only_fields_hidden_under_replay_source(self) -> None:
        # model_registry.* is authoring_only — it should not resolve when
        # queried with source="replay".
        self.assertIsNotNone(contract_for_path("model_registry.registry_file", source="authoring"))
        self.assertIsNone(contract_for_path("model_registry.registry_file", source="replay"))

    def test_replay_only_fields_hidden_under_authoring_source(self) -> None:
        self.assertIsNotNone(
            contract_for_path("resolved_model_catalog.models", source="replay"),
        )
        self.assertIsNone(
            contract_for_path("resolved_model_catalog.models", source="authoring"),
        )

    def test_resolved_model_catalog_is_meta_not_batch(self) -> None:
        """``resolved_model_catalog.*`` is intentionally NOT batch-scoped.

        Replay may legitimately select runs from different historical batches
        with different resolved catalogs; the unique-resolver must not reject
        those.  Tested explicitly to prevent accidental reclassification.
        """
        contract = contract_for_path("resolved_model_catalog.models", source="replay")
        assert contract is not None
        self.assertEqual(contract.lifecycle, "meta")
        self.assertIsNone(contract.batch_resolver)
        self.assertFalse(is_batch_scoped("resolved_model_catalog.models"))

    def test_batch_resolver_partition_matches_spec(self) -> None:
        """The registry is the single source of truth for which fields go
        through which resolver — spot-check to prevent drift."""
        unique = {c.path for c in batch_resolver_fields("unique")}
        first_wins = {c.path for c in batch_resolver_fields("first_wins")}
        self.assertEqual(
            unique,
            {"resources.max_available_gpus", "dispatch.group_overflow_policy"},
        )
        # First-wins set includes identity components + batch bookkeeping.
        # Note that ``output`` is split into three explicit entries so that
        # ``output.batch_name``'s replay-specific semantic can be documented
        # distinctly from the other two.
        self.assertIn("project", first_wins)
        self.assertIn("experiment_name", first_wins)
        self.assertIn("output.base_output_dir", first_wins)
        self.assertIn("output.batch_name", first_wins)
        self.assertIn("output.dependencies.*", first_wins)
        self.assertIn("notify.*", first_wins)
        self.assertIn("storage.*", first_wins)


# ---------------------------------------------------------------------------
# Sweep validation must route all decisions through the contract registry.
# ---------------------------------------------------------------------------


class SweepContractEnforcementTests(unittest.TestCase):
    def _validate_paths(
        self,
        *,
        shared_axes=(),
        cases_set=(),
    ) -> None:
        cases = tuple(
            SweepCaseSpec(name=name, set_values=((path, value),), axes=())
            for name, (path, value) in cases_set
        )
        spec = SweepSpec(
            enabled=True,
            shared_axes=tuple(shared_axes),
            cases=cases,
        )
        validate_batch_scoped_sweep_paths(spec, config_path=Path("/tmp/x.yaml"))

    # Rejected (batch-scoped or meta/authoring_only)
    def test_1_sweep_model_registry_registry_file_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[("model_registry.registry_file", ("a.yaml", "b.yaml"))])
        self.assertIn("model_registry.registry_file", str(ctx.exception))

    def test_2_sweep_model_registry_extra_models_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[("model_registry.extra_models", ([], []))])
        self.assertIn("model_registry.extra_models", str(ctx.exception))

    def test_3_sweep_output_base_output_dir_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[("output.base_output_dir", ("./a", "./b"))])
        self.assertIn("output.base_output_dir", str(ctx.exception))

    def test_4_sweep_storage_backend_engine_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[("storage.backend.engine", ("none", "sqlite"))])
        self.assertIn("storage.backend.engine", str(ctx.exception))

    def test_5_sweep_max_available_gpus_rejected(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[("resources.max_available_gpus", (8, 16))])
        self.assertIn("resources.max_available_gpus", str(ctx.exception))

    # Allowed (run-scoped)
    def test_6_sweep_max_gpus_per_job_allowed(self) -> None:
        self._validate_paths(shared_axes=[("resources.max_gpus_per_job", (2, 4, 8))])

    def test_7_sweep_validation_cli_args_allowed(self) -> None:
        self._validate_paths(shared_axes=[("validation.cli_args", ("warn", "error"))])

    def test_8_sweep_cluster_gpus_per_node_allowed(self) -> None:
        self._validate_paths(shared_axes=[("cluster.gpus_per_node", (1, 2, 4))])

    def test_9_sweep_env_extra_env_dynamic_key_allowed(self) -> None:
        # env.extra_env.* is a DynamicMapping; `env.*` in the registry covers
        # it, but we assert that specific dynamic keys resolve to "allowed".
        self._validate_paths(shared_axes=[("env.extra_env.MY_VAR", ("1", "0"))])

    # Mixed: offending path must be named explicitly, allowed sibling must not.
    def test_mixed_sweep_names_only_violation(self) -> None:
        with self.assertRaises(ConfigContractError) as ctx:
            self._validate_paths(shared_axes=[
                ("resources.max_available_gpus", (8, 16)),
                ("resources.max_gpus_per_job", (2, 4)),
            ])
        msg = str(ctx.exception)
        self.assertIn("resources.max_available_gpus", msg)
        self.assertNotIn("sweep.shared_axes.resources.max_gpus_per_job", msg)


# ---------------------------------------------------------------------------
# Replay batch-consistency: user-facing diagnostics, not InternalCompilerError.
# ---------------------------------------------------------------------------


def _minimal_bundle() -> MaterializedSourceBundle:
    """Smallest MaterializedSourceBundle we can pass to build_materialized_report."""
    source_report = SourceCollectionReport(
        request=AuthoringSourceRequest(config_path=Path("/tmp/x.yaml")),
        batch=SourceInputBatch(
            source_inputs=(),
            checked_inputs=0,
            manifest_extras={},
            failed_runs=(),
            batch_diagnostics=(),
            source_summary="",
        ),
    )
    return MaterializedSourceBundle(
        report=source_report,
        context=None,
        batch_diagnostics=(),
        manifest_extras={},
    )


class ReplayBatchConsistencyTests(unittest.TestCase):
    """Divergent batch-scoped scalars must surface as user-readable config
    errors (batch diagnostics), not InternalCompilerError.  ``--set``
    override unifies them by pre-merging into each run's raw cfg, which
    collapses the candidate list to a single value before resolution."""

    def _build(self, *, max_available_gpus_candidates, dispatch_policy_candidates):
        return build_materialized_report(
            materialized=_minimal_bundle(),
            identity=None,
            successful_runs=(),
            failed_runs=(),
            checked_runs=0,
            notify_cfg=None,
            submit_dependencies=None,
            max_available_gpus_candidates=max_available_gpus_candidates,
            dispatch_policy_candidates=dispatch_policy_candidates,
        )

    def test_10_max_available_gpus_disagreement_surfaces_as_batch_diagnostic(self) -> None:
        report = self._build(
            max_available_gpus_candidates=(8, 16),
            dispatch_policy_candidates=("error", "error"),
        )
        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_max_available_gpus", codes)
        # Error must be batch-level (non-runtime diagnostic), NOT a framework
        # bug — the code and category are user-actionable.
        matching = next(d for d in report.batch_diagnostics if d.code == "batch_scope_inconsistent_max_available_gpus")
        self.assertEqual(matching.stage, "batch")
        self.assertIn("--set resources.max_available_gpus=", matching.message)

    def test_11_max_available_gpus_override_via_set_unifies(self) -> None:
        # Simulate: user passed --set resources.max_available_gpus=32, so every
        # reconstructed spec carries 32.  build_materialized_report sees a
        # single unique candidate and returns 32 cleanly.
        report = self._build(
            max_available_gpus_candidates=(32, 32, 32),
            dispatch_policy_candidates=("error", "error", "error"),
        )
        self.assertEqual(report.max_available_gpus, 32)
        # No scope-inconsistency diagnostic was surfaced.
        codes = {d.code for d in report.batch_diagnostics}
        self.assertNotIn("batch_scope_inconsistent_max_available_gpus", codes)

    def test_12_dispatch_policy_disagreement_surfaces_as_batch_diagnostic(self) -> None:
        report = self._build(
            max_available_gpus_candidates=(8, 8),
            dispatch_policy_candidates=("error", "serial"),
        )
        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_dispatch_policy", codes)
        matching = next(d for d in report.batch_diagnostics if d.code == "batch_scope_inconsistent_dispatch_policy")
        self.assertIn("--set dispatch.group_overflow_policy=", matching.message)

    def test_13_dispatch_policy_override_via_set_unifies(self) -> None:
        report = self._build(
            max_available_gpus_candidates=(8, 8, 8),
            dispatch_policy_candidates=("serial", "serial", "serial"),
        )
        self.assertEqual(report.dispatch_cfg.group_overflow_policy, "serial")


# ---------------------------------------------------------------------------
# accept_replay_spec: identity / notify / output / storage mismatches must
# become user-readable diagnostics, not InternalCompilerError.
# ---------------------------------------------------------------------------


class _StubSpec:
    """Minimal stand-in for ExperimentSpec that ``accept_replay_spec`` touches."""

    def __init__(self, *, project, experiment_name, notify, output_deps, storage, max_avail, policy):
        self.project = project
        self.experiment_name = experiment_name
        self.notify = notify
        # ExperimentSpec carries output/storage; we only need the fields the
        # accept-spec code reads, so we nest via ``types.SimpleNamespace`` in tests.
        from types import SimpleNamespace
        self.output = SimpleNamespace(dependencies=output_deps, base_output_dir=Path("/tmp"), batch_name="auto")
        self.storage = storage
        self.resources = SimpleNamespace(max_available_gpus=max_avail)
        self.dispatch = SimpleNamespace(group_overflow_policy=policy)


class AcceptReplaySpecDiagnosticsTests(unittest.TestCase):
    """The four first-wins fields (identity / notify / output.dependencies /
    storage) now emit user-readable diagnostics rather than raising
    InternalCompilerError on mismatch.  This test pokes the accept function
    directly with a stub bundle; end-to-end flow is covered by integration
    tests elsewhere once the full replay pipeline is exercised."""

    def _bundle(self):
        from slurmforge.pipeline.compiler.state import MaterializedSourceBundle, ReplayMaterializedState
        return MaterializedSourceBundle(
            report=SourceCollectionReport(
                request=AuthoringSourceRequest(config_path=Path("/tmp/x.yaml")),
                batch=SourceInputBatch(
                    source_inputs=(),
                    checked_inputs=0,
                    manifest_extras={},
                    failed_runs=(),
                    batch_diagnostics=(),
                    source_summary="",
                ),
            ),
            context=ReplayMaterializedState(
                project_root_override=None,
                project_root=Path("/tmp"),
                cli_overrides=(),
                parsed_overrides=(),
                default_batch_name="b1",
                manifest_context_key=None,
            ),
            batch_diagnostics=(),
            manifest_extras={},
        )

    def _accept(self, state, spec):
        return accept_replay_spec(state, spec=spec, materialized=self._bundle(), source_input=None)

    def test_14_identity_mismatch_is_user_readable_diagnostic(self) -> None:
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        storage = StorageConfigSpec()
        notify = NotifyConfig()

        spec_a = _StubSpec(
            project="p1", experiment_name="e1",
            notify=notify, output_deps={}, storage=storage,
            max_avail=8, policy="error",
        )
        spec_b = _StubSpec(
            project="p2", experiment_name="e2",
            notify=notify, output_deps={}, storage=storage,
            max_avail=8, policy="error",
        )
        # First accept seeds state; second accept with different project
        # triggers identity mismatch — must be a diagnostic, not a raise.
        state = CompileState()
        state = self._accept(state, spec_a)
        state = self._accept(state, spec_b)  # must not raise
        codes = {d.code for d in state.batch_diagnostics}
        self.assertIn("replay_identity_mismatch", codes)
        matching = next(d for d in state.batch_diagnostics if d.code == "replay_identity_mismatch")
        # Error message names the remedy path.
        self.assertIn("--set", matching.message)


# ---------------------------------------------------------------------------
# 15. Retry / rerun flow must share the same batch-diagnostic surface
# ---------------------------------------------------------------------------


class RetryFlowSharedBatchConsistencyTests(unittest.TestCase):
    """``rerun`` reconstructs replay-style specs but enters through a
    ``RetrySourceRequest``.  The batch-scoped consistency checks must apply
    identically — a retry batch with divergent ``max_available_gpus`` across
    selected runs must surface the SAME user-readable diagnostic that the
    plain replay path produces.

    These tests are integration-style: they go through the compile-flow
    strategy layer (``REPLAY_FLOW.accept_spec``) rather than calling
    ``accept_replay_spec`` directly, so we exercise the dispatch that retry
    and replay both rely on.
    """

    def _bundle_for_request(self, request) -> MaterializedSourceBundle:
        return MaterializedSourceBundle(
            report=SourceCollectionReport(
                request=request,
                batch=SourceInputBatch(
                    source_inputs=(),
                    checked_inputs=0,
                    manifest_extras={},
                    failed_runs=(),
                    batch_diagnostics=(),
                    source_summary="",
                ),
            ),
            context=ReplayMaterializedState(
                project_root_override=None,
                project_root=Path("/tmp"),
                cli_overrides=(),
                parsed_overrides=(),
                default_batch_name="b1",
                manifest_context_key=None,
            ),
            batch_diagnostics=(),
            manifest_extras={},
        )

    def _divergent_specs(self):
        """Two stub specs that agree on identity but diverge on
        ``resources.max_available_gpus``.  The replay/rerun resolver must
        catch this as a batch-level error."""
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        storage = StorageConfigSpec()
        notify = NotifyConfig()
        common = dict(
            project="proj", experiment_name="exp",
            notify=notify, output_deps={}, storage=storage, policy="error",
        )
        return (
            _StubSpec(max_avail=8, **common),
            _StubSpec(max_avail=16, **common),
        )

    def test_retry_source_request_dispatches_to_replay_flow(self) -> None:
        """Structural guarantee: the compiler strategy registry routes
        RetrySourceRequest through the same flow as ReplaySourceRequest,
        so any fix to replay consistency automatically reaches retry."""
        from slurmforge.pipeline.compiler.flows import REPLAY_FLOW
        from slurmforge.pipeline.compiler.requests import (
            ReplaySourceRequest,
            RetrySourceRequest,
        )

        self.assertIn(RetrySourceRequest, REPLAY_FLOW.request_types)
        self.assertIn(ReplaySourceRequest, REPLAY_FLOW.request_types)

    def test_retry_bundle_surfaces_same_batch_scalar_diagnostic_as_replay(self) -> None:
        from slurmforge.pipeline.compiler.flows import REPLAY_FLOW
        from slurmforge.pipeline.compiler.requests import RetrySourceRequest

        retry_bundle = self._bundle_for_request(
            RetrySourceRequest(source_batch_root=Path("/tmp/batch"), status_query="failed"),
        )
        spec_a, spec_b = self._divergent_specs()

        # Enter via the flow-level accept_spec so we exercise the retry
        # dispatch path exactly as the compile engine would.
        state = CompileState()
        state = REPLAY_FLOW.accept_spec(state, spec=spec_a, materialized=retry_bundle, source_input=None)
        state = REPLAY_FLOW.accept_spec(state, spec=spec_b, materialized=retry_bundle, source_input=None)

        # Candidates accumulated identically to the replay case.
        self.assertEqual(state.max_available_gpus_candidates, (8, 16))
        # The report builder then turns the divergence into a batch diagnostic —
        # the SAME diagnostic code the replay path produces.
        report = build_materialized_report(
            materialized=retry_bundle,
            identity=state.identity,
            successful_runs=(),
            failed_runs=(),
            checked_runs=0,
            notify_cfg=state.notify_cfg,
            submit_dependencies=state.submit_dependencies,
            max_available_gpus_candidates=state.max_available_gpus_candidates,
            dispatch_policy_candidates=state.dispatch_policy_candidates,
        )
        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_max_available_gpus", codes)

    def test_retry_bundle_surfaces_same_identity_diagnostic_as_replay(self) -> None:
        from slurmforge.pipeline.compiler.flows import REPLAY_FLOW
        from slurmforge.pipeline.compiler.requests import RetrySourceRequest
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        retry_bundle = self._bundle_for_request(
            RetrySourceRequest(source_batch_root=Path("/tmp/batch"), status_query="failed"),
        )
        storage = StorageConfigSpec()
        notify = NotifyConfig()
        spec_a = _StubSpec(
            project="p1", experiment_name="e1",
            notify=notify, output_deps={}, storage=storage,
            max_avail=8, policy="error",
        )
        spec_b = _StubSpec(
            project="p2", experiment_name="e2",
            notify=notify, output_deps={}, storage=storage,
            max_avail=8, policy="error",
        )

        state = CompileState()
        state = REPLAY_FLOW.accept_spec(state, spec=spec_a, materialized=retry_bundle, source_input=None)
        state = REPLAY_FLOW.accept_spec(state, spec=spec_b, materialized=retry_bundle, source_input=None)

        codes = {d.code for d in state.batch_diagnostics}
        # SAME code the plain replay path emits — not InternalCompilerError.
        self.assertIn("replay_identity_mismatch", codes)


# ---------------------------------------------------------------------------
# 16. resolved_model_catalog divergence must NOT trigger the batch resolver.
# ---------------------------------------------------------------------------


class ResolvedModelCatalogDivergenceTests(unittest.TestCase):
    """Replay/rerun can legitimately pull runs from different historical
    batches whose ``resolved_model_catalog`` differs.  The batch-consistency
    machinery must ignore that divergence completely — only each run's own
    catalog matters.

    These tests prove it from two angles:

      (a) ``accept_replay_spec`` reads only batch-scoped scalars and the
          four first-wins fields.  It must not touch ``spec.model_catalog``.
      (b) The contract registry refuses to mark ``resolved_model_catalog.*``
          as a batch resolver candidate, so no future refactor can quietly
          start checking it.
    """

    def _bundle(self) -> MaterializedSourceBundle:
        return MaterializedSourceBundle(
            report=SourceCollectionReport(
                request=AuthoringSourceRequest(config_path=Path("/tmp/x.yaml")),
                batch=SourceInputBatch(
                    source_inputs=(),
                    checked_inputs=0,
                    manifest_extras={},
                    failed_runs=(),
                    batch_diagnostics=(),
                    source_summary="",
                ),
            ),
            context=ReplayMaterializedState(
                project_root_override=None,
                project_root=Path("/tmp"),
                cli_overrides=(),
                parsed_overrides=(),
                default_batch_name="b1",
                manifest_context_key=None,
            ),
            batch_diagnostics=(),
            manifest_extras={},
        )

    def _stub_spec_with_catalog(self, catalog_marker):
        """Stub spec mirroring what ``accept_replay_spec`` reads plus a
        divergent ``model_catalog`` payload — the latter is there ONLY to
        prove it does not trip any resolver."""
        from types import SimpleNamespace

        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        spec = _StubSpec(
            project="p", experiment_name="e",
            notify=NotifyConfig(), output_deps={}, storage=StorageConfigSpec(),
            max_avail=8, policy="error",
        )
        # Attach a divergent catalog payload — nothing in the batch-consistency
        # path looks at it, so these runs must still pass.
        spec.model_catalog = SimpleNamespace(models=catalog_marker)
        return spec

    def test_divergent_catalog_does_not_produce_diagnostic_in_accept_spec(self) -> None:
        bundle = self._bundle()
        spec_a = self._stub_spec_with_catalog(catalog_marker=("resnet50@v1",))
        spec_b = self._stub_spec_with_catalog(catalog_marker=("gpt2@v3", "bert@v2"))

        state = CompileState()
        state = accept_replay_spec(state, spec=spec_a, materialized=bundle, source_input=None)
        state = accept_replay_spec(state, spec=spec_b, materialized=bundle, source_input=None)

        # No diagnostic should reference catalog divergence.  All codes that
        # DO appear must belong to something else entirely (if anything).
        forbidden_codes = {d.code for d in state.batch_diagnostics if "catalog" in d.code}
        self.assertEqual(forbidden_codes, set())
        # And more specifically, the batch-scalar candidates converged (both
        # specs carry the same max_available_gpus/policy), so the report
        # builder returns a clean result.
        report = build_materialized_report(
            materialized=bundle,
            identity=state.identity,
            successful_runs=(),
            failed_runs=(),
            checked_runs=0,
            notify_cfg=state.notify_cfg,
            submit_dependencies=state.submit_dependencies,
            max_available_gpus_candidates=state.max_available_gpus_candidates,
            dispatch_policy_candidates=state.dispatch_policy_candidates,
        )
        report_codes = {d.code for d in report.batch_diagnostics}
        self.assertFalse(
            any("catalog" in code for code in report_codes),
            f"unexpected catalog-related diagnostic in report: {report_codes}",
        )

    def test_resolved_model_catalog_paths_never_appear_in_batch_resolver_fields(self) -> None:
        """Structural guard: no batch resolver field shares a prefix with
        ``resolved_model_catalog``.  This catches any future contract edit
        that accidentally reclassifies the catalog as batch-scoped."""
        for contract in batch_resolver_fields():
            self.assertFalse(
                contract.path.startswith("resolved_model_catalog"),
                f"resolved_model_catalog path leaked into batch resolver: {contract.path}",
            )


# ---------------------------------------------------------------------------
# Registry ↔ replay-checker alignment.
#
# The replay first-wins checker in ``flows/replay/identity.py`` hand-writes
# four comparisons (identity / notify / submit_dependencies / storage).  This
# test locks the registry's ``first_wins`` set to the exact contract paths
# those comparisons cover, so a future registry edit that adds a new
# first-wins contract without extending the checker fails CI loudly.
# ---------------------------------------------------------------------------


_EXPECTED_REPLAY_FIRST_WINS_PATHS = frozenset({
    "project",
    "experiment_name",
    "output.base_output_dir",
    "output.batch_name",
    "output.dependencies.*",
    "notify.*",
    "storage.*",
})


class FirstWinsRegistryAlignmentTests(unittest.TestCase):
    def test_first_wins_registry_matches_replay_checker_coverage(self) -> None:
        registry_paths = {c.path for c in batch_resolver_fields("first_wins")}
        self.assertEqual(
            registry_paths,
            _EXPECTED_REPLAY_FIRST_WINS_PATHS,
            (
                "registry first_wins set drifted from the replay checker's "
                "hand-written comparisons. Either update the replay checker "
                "in pipeline/compiler/flows/replay/identity.py to cover the "
                "new field, or update _EXPECTED_REPLAY_FIRST_WINS_PATHS here."
            ),
        )


# ---------------------------------------------------------------------------
# output.batch_name: replay semantic lock-in.
#
# In replay/rerun, the original batch_name on each source run is NOT a
# first-wins constraint: the new batch always uses ``default_batch_name``
# unless the user explicitly passes ``--set output.batch_name=...``.
# We assert this by pointing ``resolve_replay_batch_identity`` at two specs
# whose stored batch_names differ but project/experiment/base_output_dir
# agree: no identity mismatch may surface.
# ---------------------------------------------------------------------------


class OutputBatchNameReplaySemanticTests(unittest.TestCase):
    def _replay_bundle_with_default(self, default_batch_name: str) -> MaterializedSourceBundle:
        return MaterializedSourceBundle(
            report=SourceCollectionReport(
                request=AuthoringSourceRequest(config_path=Path("/tmp/x.yaml")),
                batch=SourceInputBatch(
                    source_inputs=(),
                    checked_inputs=0,
                    manifest_extras={},
                    failed_runs=(),
                    batch_diagnostics=(),
                    source_summary="",
                ),
            ),
            context=ReplayMaterializedState(
                project_root_override=None,
                project_root=Path("/tmp"),
                cli_overrides=(),
                parsed_overrides=(),
                default_batch_name=default_batch_name,
                manifest_context_key=None,
            ),
            batch_diagnostics=(),
            manifest_extras={},
        )

    def test_divergent_stored_batch_name_without_cli_override_does_not_trigger_identity_mismatch(self) -> None:
        """Two source runs carry different historical batch_name values.
        Because no ``--set output.batch_name=...`` was passed, the compile
        flow unifies them under the shared ``default_batch_name`` — identity
        derives from project+experiment+default, so the two specs resolve
        to the same identity and no replay_identity_mismatch surfaces."""
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig
        from types import SimpleNamespace

        storage = StorageConfigSpec()
        notify = NotifyConfig()
        bundle = self._replay_bundle_with_default("new_batch_2026")

        # Two stubs identical on every batch-identity component except the
        # stored batch_name (which is the historical batch they came from).
        def make(stored_batch_name: str) -> _StubSpec:
            spec = _StubSpec(
                project="p", experiment_name="e",
                notify=notify, output_deps={}, storage=storage,
                max_avail=8, policy="error",
            )
            spec.output = SimpleNamespace(
                dependencies={},
                base_output_dir=Path("/tmp"),
                batch_name=stored_batch_name,
            )
            return spec

        spec_a = make("old_batch_a")
        spec_b = make("old_batch_b")

        state = CompileState()
        state = accept_replay_spec(state, spec=spec_a, materialized=bundle, source_input=None)
        state = accept_replay_spec(state, spec=spec_b, materialized=bundle, source_input=None)

        codes = {d.code for d in state.batch_diagnostics}
        self.assertNotIn(
            "replay_identity_mismatch", codes,
            "stored batch_name divergence must not trigger identity mismatch — "
            "default_batch_name is the authoritative name for replay batches.",
        )


# ---------------------------------------------------------------------------
# Replay / retry integration: first-wins mismatches become user-facing
# diagnostics (category=config), and any error clears both successful_runs
# AND gpu_budget_plan in the report.
# ---------------------------------------------------------------------------


class ReplayFirstWinsIntegrationTests(unittest.TestCase):
    """Exercise each first-wins mismatch via the full REPLAY_FLOW.accept_spec
    strategy layer and the report builder that follows.  Covers:

      - notify mismatch → replay_notify_mismatch
      - output.dependencies mismatch → replay_submit_dependencies_mismatch
      - storage mismatch → replay_storage_mismatch
      - ANY error → report.successful_runs == () AND report.gpu_budget_plan is None
      - diagnostic category is "config", not "resource"
    """

    def _replay_bundle(self) -> MaterializedSourceBundle:
        return MaterializedSourceBundle(
            report=SourceCollectionReport(
                request=AuthoringSourceRequest(config_path=Path("/tmp/x.yaml")),
                batch=SourceInputBatch(
                    source_inputs=(),
                    checked_inputs=0,
                    manifest_extras={},
                    failed_runs=(),
                    batch_diagnostics=(),
                    source_summary="",
                ),
            ),
            context=ReplayMaterializedState(
                project_root_override=None,
                project_root=Path("/tmp"),
                cli_overrides=(),
                parsed_overrides=(),
                default_batch_name="b1",
                manifest_context_key=None,
            ),
            batch_diagnostics=(),
            manifest_extras={},
        )

    def _run_via_flow(self, spec_a, spec_b, *, successful_runs=()):
        """Accept two specs through REPLAY_FLOW.accept_spec (as the compile
        engine would) and then build the full materialized report.

        ``successful_runs`` defaults to ``()`` because most tests here only
        need to verify the diagnostic routing, and ``_compute_budget_plan``
        would iterate non-trivial PlannedRun structure we don't need to
        synthesize.  The clearance-on-truthy-runs guarantee is covered
        separately by ``test_max_available_gpus_mismatch_clears_non_empty_successful_runs``
        on a code path where the resolver fails BEFORE budget computation runs.
        """
        from slurmforge.pipeline.compiler.flows import REPLAY_FLOW

        bundle = self._replay_bundle()
        state = CompileState()
        state = REPLAY_FLOW.accept_spec(state, spec=spec_a, materialized=bundle, source_input=None)
        state = REPLAY_FLOW.accept_spec(state, spec=spec_b, materialized=bundle, source_input=None)

        report = build_materialized_report(
            materialized=bundle,
            identity=state.identity,
            successful_runs=successful_runs,
            failed_runs=(),
            checked_runs=len(successful_runs),
            notify_cfg=state.notify_cfg,
            submit_dependencies=state.submit_dependencies,
            batch_diagnostics=state.batch_diagnostics,
            max_available_gpus_candidates=state.max_available_gpus_candidates,
            dispatch_policy_candidates=state.dispatch_policy_candidates,
        )
        return state, report

    def _divergent_pair(self, *, which: str):
        """Two stub specs that diverge only on the named first-wins field."""
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        default_notify = NotifyConfig()
        default_storage = StorageConfigSpec()
        base_kwargs = dict(
            project="p", experiment_name="e",
            notify=default_notify, output_deps={}, storage=default_storage,
            max_avail=8, policy="error",
        )

        spec_a = _StubSpec(**base_kwargs)
        spec_b = _StubSpec(**base_kwargs)
        if which == "notify":
            # Construct a distinct NotifyConfig.  The equality check is
            # structural — differing .email is enough.
            spec_b.notify = NotifyConfig(enabled=True, email="other@example.com", when="afterok")
        elif which == "output.dependencies":
            spec_b.output.dependencies = {"afterok": ["9999"]}
        elif which == "storage":
            from slurmforge.pipeline.config.models.storage import StorageBackendConfig
            spec_b.storage = StorageConfigSpec(
                backend=StorageBackendConfig(engine="sqlite"),
            )
        else:
            raise ValueError(f"unknown divergence: {which!r}")
        return spec_a, spec_b

    def _assert_error_clears_plan(self, report) -> None:
        """Every batch-level error diagnostic must clear BOTH successful_runs
        and gpu_budget_plan.  This guards against leakage of plan artifacts
        into materialization when the contract is broken."""
        self.assertEqual(
            report.successful_runs, (),
            "batch-level error must clear successful_runs",
        )
        self.assertIsNone(
            report.gpu_budget_plan,
            "batch-level error must clear gpu_budget_plan",
        )

    def test_notify_mismatch_surfaces_config_diagnostic_and_clears_plan(self) -> None:
        spec_a, spec_b = self._divergent_pair(which="notify")
        _, report = self._run_via_flow(spec_a, spec_b)

        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("replay_notify_mismatch", codes)

        diag = next(d for d in report.batch_diagnostics if d.code == "replay_notify_mismatch")
        # Category must be "config" — this is a contract violation, not a
        # resource/memory issue.
        self.assertEqual(str(diag.category), "config")
        self.assertEqual(str(diag.severity), "error")
        self.assertIn("--set", diag.message)

        self._assert_error_clears_plan(report)

    def test_submit_dependencies_mismatch_surfaces_config_diagnostic_and_clears_plan(self) -> None:
        spec_a, spec_b = self._divergent_pair(which="output.dependencies")
        _, report = self._run_via_flow(spec_a, spec_b)

        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("replay_submit_dependencies_mismatch", codes)

        diag = next(d for d in report.batch_diagnostics if d.code == "replay_submit_dependencies_mismatch")
        self.assertEqual(str(diag.category), "config")
        self.assertIn("--set", diag.message)

        self._assert_error_clears_plan(report)

    def test_storage_mismatch_surfaces_config_diagnostic_and_clears_plan(self) -> None:
        spec_a, spec_b = self._divergent_pair(which="storage")
        _, report = self._run_via_flow(spec_a, spec_b)

        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("replay_storage_mismatch", codes)

        diag = next(d for d in report.batch_diagnostics if d.code == "replay_storage_mismatch")
        self.assertEqual(str(diag.category), "config")
        self.assertIn("--set", diag.message)

        self._assert_error_clears_plan(report)

    def test_max_available_gpus_disagreement_also_clears_plan(self) -> None:
        """Batch-scope unique resolver errors take the same unified clearance
        path as first-wins errors — defense against the prior bug where only
        the scattered clears fired."""
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        notify = NotifyConfig()
        storage = StorageConfigSpec()
        common = dict(
            project="p", experiment_name="e",
            notify=notify, output_deps={}, storage=storage, policy="error",
        )
        spec_a = _StubSpec(max_avail=8, **common)
        spec_b = _StubSpec(max_avail=16, **common)
        _, report = self._run_via_flow(spec_a, spec_b)

        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("batch_scope_inconsistent_max_available_gpus", codes)
        self._assert_error_clears_plan(report)

    def test_max_available_gpus_mismatch_clears_non_empty_successful_runs(self) -> None:
        """Stronger clearance proof.

        Feed the builder a non-empty ``successful_runs`` sequence on a path
        where the error fires BEFORE the budget planner iterates the list
        (resolver-level mismatch short-circuits ``_compute_budget_plan``).
        The unified error gate in ``build_materialized_report`` must still
        reduce ``report.successful_runs`` to ``()`` and leave
        ``report.gpu_budget_plan`` as ``None`` — otherwise materialization
        would see a plan inconsistent with the contract error the user
        still has to resolve.
        """
        from slurmforge.pipeline.config.api import StorageConfigSpec
        from slurmforge.pipeline.config.runtime import NotifyConfig

        notify = NotifyConfig()
        storage = StorageConfigSpec()
        common = dict(
            project="p", experiment_name="e",
            notify=notify, output_deps={}, storage=storage, policy="error",
        )
        spec_a = _StubSpec(max_avail=8, **common)
        spec_b = _StubSpec(max_avail=16, **common)
        # Sentinels stand in for PlannedRun objects.  Safe because the
        # resolver failure short-circuits before _compute_budget_plan iterates.
        fake_runs = (object(), object(), object())
        _, report = self._run_via_flow(spec_a, spec_b, successful_runs=fake_runs)

        self.assertEqual(report.successful_runs, ())
        self.assertIsNone(report.gpu_budget_plan)

    def test_early_gate_skips_budget_computation_on_upstream_error(self) -> None:
        """Structural proof that ``build_materialized_report`` does NOT call
        ``_compute_budget_plan`` once the input bundle already carries an
        error diagnostic.

        Setup: notify mismatch (upstream error in ``batch_diagnostics``) +
        uniform batch-scope candidates (resolver succeeds, so the old
        "resolver-None short-circuit" does NOT fire here) + sentinel
        ``successful_runs`` that would crash ``_compute_budget_plan`` the
        moment it iterated them.  If the builder reached the budget
        planner, the test would raise ``AttributeError`` inside
        ``_extract_raw_groups`` ("'object' object has no attribute 'plan'").
        Clean completion proves the early gate fired first.
        """
        spec_a, spec_b = self._divergent_pair(which="notify")
        # Non-empty sentinel runs — would crash ``_compute_budget_plan``
        # if the early gate were absent.
        fake_runs = (object(), object(), object())
        _, report = self._run_via_flow(spec_a, spec_b, successful_runs=fake_runs)

        # The run completed without AttributeError, so the budget planner
        # was NOT invoked.  Further assertions lock the observable state.
        codes = {d.code for d in report.batch_diagnostics}
        self.assertIn("replay_notify_mismatch", codes)
        self.assertEqual(report.successful_runs, ())
        self.assertIsNone(report.gpu_budget_plan)

        # No secondary budget warnings must appear — this is what the
        # early gate is specifically designed to suppress.  best_effort /
        # serial-chain / MaxArraySize warnings only arise from a
        # ``_compute_budget_plan`` run; their absence is the structural
        # signal that the gate short-circuited.
        budget_warning_codes = {
            "best_effort_no_strict_global_limit",
            "serial_chain_long",
            "throttle_clamped_by_max_array_size",
        }
        self.assertFalse(
            codes & budget_warning_codes,
            f"budget warnings leaked past the early gate: {codes & budget_warning_codes}",
        )


# ---------------------------------------------------------------------------
# Acceptance: registry must not lose any contract across refactors.
# ---------------------------------------------------------------------------


class RegistryIntegrityTests(unittest.TestCase):
    def test_all_contracts_have_well_formed_paths(self) -> None:
        for contract in all_contracts():
            self.assertTrue(contract.path, "empty path")
            self.assertIn(
                contract.lifecycle,
                ("batch", "run", "meta"),
                f"bad lifecycle on {contract.path}",
            )
            self.assertIn(
                contract.source,
                ("authoring_only", "replay_only", "both"),
                f"bad source on {contract.path}",
            )
            if contract.batch_resolver is not None:
                self.assertEqual(
                    contract.lifecycle, "batch",
                    f"non-batch contract {contract.path} cannot set batch_resolver",
                )

    def test_only_batch_fields_have_resolvers(self) -> None:
        for contract in all_contracts():
            if contract.batch_resolver is not None:
                self.assertEqual(contract.lifecycle, "batch")


if __name__ == "__main__":
    unittest.main()
