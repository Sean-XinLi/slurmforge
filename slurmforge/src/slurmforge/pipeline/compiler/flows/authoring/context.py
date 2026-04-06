from __future__ import annotations

from .....model_support.catalog import ModelCatalogResolver
from ...state import AuthoringMaterializedState, CollectedSourceBundle, MaterializedSourceBundle
from ...requests import AuthoringSourceRequest


def materialize_authoring_bundle(bundle: CollectedSourceBundle) -> MaterializedSourceBundle:
    report = bundle.report
    request = report.request
    assert isinstance(request, AuthoringSourceRequest)
    if bundle.payload is None:
        return MaterializedSourceBundle(
            report=report,
            context=None,
            batch_diagnostics=report.batch_diagnostics,
            manifest_extras=report.manifest_extras,
        )

    payload = bundle.payload
    return MaterializedSourceBundle(
        report=report,
        context=AuthoringMaterializedState(
            config_path=payload.config_path,
            project_root=payload.project_root,
            shared=payload.shared,
            model_catalog_resolver=ModelCatalogResolver(payload.project_root),
            default_batch_name=str(request.default_batch_name or ""),
        ),
        batch_diagnostics=report.batch_diagnostics,
        manifest_extras=report.manifest_extras,
    )
