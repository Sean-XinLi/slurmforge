from __future__ import annotations

from typing import Any

from ..config_contract.registry import default_for
from ..config_contract.keys import reject_unknown_config_keys
from .models import ArtifactStoreSpec
from .parse_common import optional_mapping

DEFAULT_ARTIFACT_STORE_STRATEGY = default_for("artifact_store.strategy")
DEFAULT_ARTIFACT_STORE_FALLBACK_STRATEGY = default_for(
    "artifact_store.fallback_strategy"
)
DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST = default_for("artifact_store.verify_digest")
DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR = default_for(
    "artifact_store.fail_on_verify_error"
)


def parse_artifact_store(raw: Any) -> ArtifactStoreSpec:
    data = optional_mapping(raw, "artifact_store")
    reject_unknown_config_keys(data, parent="artifact_store")
    fallback = data.get("fallback_strategy")
    return ArtifactStoreSpec(
        strategy=str(data.get("strategy") or DEFAULT_ARTIFACT_STORE_STRATEGY),
        fallback_strategy=(
            DEFAULT_ARTIFACT_STORE_FALLBACK_STRATEGY
            if fallback in (None, "")
            else str(fallback)
        ),
        verify_digest=bool(
            data.get("verify_digest", DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST)
        ),
        fail_on_verify_error=bool(
            data.get(
                "fail_on_verify_error",
                DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR,
            )
        ),
    )
