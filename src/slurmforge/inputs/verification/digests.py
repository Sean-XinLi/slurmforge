from __future__ import annotations

from ...contracts import InputBinding
from ...io import content_digest


def normalize_digest(value: object) -> str:
    if value in (None, ""):
        return ""
    digest = str(value)
    return digest.split(":", 1)[1] if digest.startswith("sha256:") else digest


def producer_digest(binding: InputBinding) -> str:
    resolution = binding.resolution
    return normalize_digest(
        binding.resolved.digest
        or resolution.output_digest
        or resolution.producer_digest
        or resolution.digest
    )


def expected_digest(binding: InputBinding) -> str:
    resolution = binding.resolution
    explicit = normalize_digest(resolution.expected_digest)
    if explicit:
        return explicit
    if binding.resolved.kind in {"path", "manifest"}:
        return producer_digest(binding)
    return ""


def value_digest(value: object) -> str:
    return content_digest(value)
