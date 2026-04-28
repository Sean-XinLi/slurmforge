from __future__ import annotations

from ...contracts import InputBinding
from ...io import content_digest


def normalize_digest(value: object) -> str:
    if value in (None, ""):
        return ""
    digest = str(value)
    return digest.split(":", 1)[1] if digest.startswith("sha256:") else digest


def producer_digest(binding: InputBinding) -> str:
    resolution = dict(binding.resolution or {})
    return normalize_digest(
        binding.resolved.digest
        or resolution.get("output_digest")
        or resolution.get("producer_digest")
        or resolution.get("digest")
    )


def expected_digest(binding: InputBinding) -> str:
    resolution = dict(binding.resolution or {})
    explicit = normalize_digest(
        resolution.get("expected_digest")
        or resolution.get("expected_value_digest")
        or resolution.get("value_digest")
    )
    if explicit:
        return explicit
    if binding.resolved.kind in {"path", "manifest"}:
        return producer_digest(binding)
    return ""


def value_digest(value: object) -> str:
    return content_digest(value)
