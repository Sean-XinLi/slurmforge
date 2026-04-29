from __future__ import annotations

from typing import Any

from ..config_contract.defaults import (
    DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR,
    DEFAULT_ARTIFACT_STORE_FALLBACK_STRATEGY,
    DEFAULT_ARTIFACT_STORE_STRATEGY,
    DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST,
)
from ..config_schema import reject_unknown_config_keys
from .models import ArtifactStoreSpec
from .parse_common import optional_mapping


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
