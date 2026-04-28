from __future__ import annotations

from typing import Any

from .models import ArtifactStoreSpec
from .parse_common import optional_mapping


def parse_artifact_store(raw: Any) -> ArtifactStoreSpec:
    data = optional_mapping(raw, "artifact_store")
    fallback = data.get("fallback_strategy")
    return ArtifactStoreSpec(
        strategy=str(data.get("strategy") or "copy"),
        fallback_strategy=None if fallback in (None, "") else str(fallback),
        verify_digest=bool(data.get("verify_digest", True)),
        fail_on_verify_error=bool(data.get("fail_on_verify_error", True)),
    )
