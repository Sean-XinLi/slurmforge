from __future__ import annotations

from typing import Any

from ..config_contract.keys import reject_unknown_config_keys
from ..config_contract.registry import default_for
from .models import ArtifactStoreSpec
from .parse_common import optional_mapping


def parse_artifact_store(raw: Any) -> ArtifactStoreSpec:
    data = optional_mapping(raw, "artifact_store")
    reject_unknown_config_keys(data, parent="artifact_store")
    fallback = data.get("fallback_strategy")
    return ArtifactStoreSpec(
        strategy=str(data.get("strategy") or default_for("artifact_store.strategy")),
        fallback_strategy=(
            default_for("artifact_store.fallback_strategy")
            if fallback in (None, "")
            else str(fallback)
        ),
        verify_digest=bool(
            data.get("verify_digest", default_for("artifact_store.verify_digest"))
        ),
        fail_on_verify_error=bool(
            data.get(
                "fail_on_verify_error",
                default_for("artifact_store.fail_on_verify_error"),
            )
        ),
    )
