from __future__ import annotations

from typing import Any, Sequence

from ...config.api import StorageConfigSpec
from ...config.runtime import NotifyConfig
from ...planning import BatchIdentity, PlannedRun
from ...planning.contracts import PlanDiagnostic
from ...sources.models import FailedCompiledRun
from ..state import MaterializedSourceBundle
from .models import BatchCompileReport
from .validator import validate_compile_report


def build_report(
    *,
    identity: BatchIdentity | None,
    successful_runs: Sequence[PlannedRun],
    failed_runs: Sequence[FailedCompiledRun],
    batch_diagnostics: Sequence[PlanDiagnostic],
    checked_runs: int,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]] | None,
    manifest_extras: dict[str, Any],
    source_summary: str,
    storage_config: StorageConfigSpec | None = None,
) -> BatchCompileReport:
    return validate_compile_report(
        BatchCompileReport(
            identity=identity,
            successful_runs=tuple(successful_runs),
            failed_runs=tuple(failed_runs),
            batch_diagnostics=tuple(batch_diagnostics),
            checked_runs=checked_runs,
            notify_cfg=notify_cfg,
            submit_dependencies={} if submit_dependencies is None else submit_dependencies,
            manifest_extras=manifest_extras,
            source_summary=source_summary,
            storage_config=storage_config if storage_config is not None else StorageConfigSpec(),
        )
    )


def build_materialized_report(
    *,
    materialized: MaterializedSourceBundle,
    identity: BatchIdentity | None,
    successful_runs: Sequence[PlannedRun],
    failed_runs: Sequence[FailedCompiledRun],
    batch_diagnostics: Sequence[PlanDiagnostic] | None = None,
    checked_runs: int,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]] | None,
    storage_config: StorageConfigSpec | None = None,
) -> BatchCompileReport:
    return build_report(
        identity=identity,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        batch_diagnostics=materialized.batch_diagnostics if batch_diagnostics is None else batch_diagnostics,
        checked_runs=checked_runs,
        notify_cfg=notify_cfg,
        submit_dependencies=submit_dependencies,
        manifest_extras=materialized.manifest_extras,
        source_summary=materialized.report.source_summary,
        storage_config=storage_config,
    )


def build_compile_failure_report(
    *,
    materialized: MaterializedSourceBundle,
    failed_runs: Sequence[FailedCompiledRun],
    checked_runs: int,
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]] | None,
    exc: Exception,
    category: str,
    code: str,
    stage: str = "batch",
    diagnostics_from_exception,
) -> BatchCompileReport:
    return build_materialized_report(
        materialized=materialized,
        identity=None,
        successful_runs=(),
        failed_runs=failed_runs,
        batch_diagnostics=diagnostics_from_exception(exc, category=category, code=code, stage=stage),
        checked_runs=checked_runs,
        notify_cfg=notify_cfg,
        submit_dependencies=submit_dependencies,
    )
