from __future__ import annotations

import os
from pathlib import Path

from ..io import content_digest, file_digest, to_jsonable
from ..contracts import (
    InputBinding,
    binding_is_ready_for_injection,
    resolved_kind_matches_expectation,
    resolved_payload_present,
)
from .models import InputVerificationRecord


def _normalize_digest(value: object) -> str:
    if value in (None, ""):
        return ""
    digest = str(value)
    return digest.split(":", 1)[1] if digest.startswith("sha256:") else digest


def _producer_digest(binding: InputBinding) -> str:
    resolution = dict(binding.resolution or {})
    return _normalize_digest(
        binding.resolved.digest
        or resolution.get("output_digest")
        or resolution.get("producer_digest")
        or resolution.get("digest")
    )


def _expected_digest(binding: InputBinding) -> str:
    resolution = dict(binding.resolution or {})
    explicit = _normalize_digest(
        resolution.get("expected_digest")
        or resolution.get("expected_value_digest")
        or resolution.get("value_digest")
    )
    if explicit:
        return explicit
    if binding.resolved.kind in {"path", "manifest"}:
        return _producer_digest(binding)
    return ""


def _path_kind(binding: InputBinding) -> str:
    resolution = dict(binding.resolution or {})
    if binding.resolved.kind == "manifest":
        return "file"
    return str(resolution.get("path_kind") or "file")


def _value_digest(value: object) -> str:
    return content_digest(value)


def _base_record(
    binding: InputBinding,
    *,
    phase: str,
    now: str,
    required: bool,
    expected_digest: str,
) -> dict[str, object]:
    resolved = binding.resolved
    return {
        "input_name": binding.input_name,
        "source": to_jsonable(binding.source),
        "expects": binding.expects,
        "resolved_kind": resolved.kind,
        "resolved_path": resolved.path or None,
        "required": required,
        "expected_digest": expected_digest,
        "producer_digest": _producer_digest(binding),
        "producer_stage_instance_id": resolved.producer_stage_instance_id,
        "verified_at": now,
        "phase": phase,
    }


def record_for_binding(
    binding: InputBinding,
    *,
    phase: str,
    now: str,
) -> InputVerificationRecord:
    required = bool(binding.inject.get("required"))
    expected_digest = _expected_digest(binding)
    base = _base_record(
        binding,
        phase=phase,
        now=now,
        required=required,
        expected_digest=expected_digest,
    )
    if not resolved_payload_present(binding):
        return InputVerificationRecord(
            **base,
            path_kind="unknown",
            state="failed" if required else "skipped",
            failure_class="input_contract_error" if required else None,
            reason="required input did not resolve" if required else "optional input is unresolved",
        )
    if not resolved_kind_matches_expectation(binding.resolved.kind, binding.expects):
        return InputVerificationRecord(
            **base,
            path_kind="unknown",
            state="failed" if required else "skipped",
            failure_class="input_contract_error" if required else None,
            reason=f"resolved kind `{binding.resolved.kind}` does not satisfy expects `{binding.expects}`",
        )
    if not binding_is_ready_for_injection(binding):
        return InputVerificationRecord(
            **base,
            path_kind="unknown",
            state="failed" if required else "skipped",
            failure_class="input_contract_error" if required else None,
            reason=f"input `{binding.input_name}` is not compatible with inject.mode={binding.inject.get('mode') or 'path'}",
        )
    if binding.resolved.kind == "value":
        value_digest = _value_digest(binding.resolved.value)
        if expected_digest and value_digest != expected_digest:
            return InputVerificationRecord(
                **base,
                path_kind="value",
                value_digest=value_digest,
                state="failed",
                failure_class="input_contract_error",
                reason=f"input value digest mismatch: expected {expected_digest}, got {value_digest}",
            )
        return InputVerificationRecord(
            **base,
            path_kind="value",
            value_digest=value_digest,
            state="verified",
            reason="verified value",
        )
    path_kind = _path_kind(binding)
    path = Path(binding.resolved.path).expanduser()
    exists = path.exists()
    is_valid_kind = exists and (path.is_file() if path_kind == "file" else path.is_dir() if path_kind == "dir" else True)
    readable = is_valid_kind and os.access(path, os.R_OK)
    size_bytes = path.stat().st_size if is_valid_kind and path.is_file() else None
    if not exists:
        state = "failed" if required else "skipped"
        return InputVerificationRecord(
            **base,
            path_kind=path_kind,
            exists=False,
            readable=False,
            size_bytes=None,
            state=state,
            failure_class="input_contract_error" if required else None,
            reason=f"input path does not exist: {path}",
        )
    if not is_valid_kind:
        state = "failed" if required else "skipped"
        return InputVerificationRecord(
            **base,
            path_kind=path_kind,
            exists=True,
            readable=False,
            size_bytes=None,
            state=state,
            failure_class="input_contract_error" if required else None,
            reason=f"input path is not a {path_kind}: {path}",
        )
    if not readable:
        state = "failed" if required else "skipped"
        return InputVerificationRecord(
            **base,
            path_kind=path_kind,
            exists=True,
            readable=False,
            size_bytes=size_bytes,
            state=state,
            failure_class="input_contract_error" if required else None,
            reason=f"input path is not readable: {path}",
        )
    digest = file_digest(path) if expected_digest and path.is_file() else ""
    if expected_digest and digest != expected_digest:
        return InputVerificationRecord(
            **base,
            path_kind=path_kind,
            exists=True,
            readable=True,
            size_bytes=size_bytes,
            digest=digest,
            state="failed",
            failure_class="input_contract_error",
            reason=f"input digest mismatch: expected {expected_digest}, got {digest}",
        )
    return InputVerificationRecord(
        **base,
        path_kind=path_kind,
        exists=True,
        readable=True,
        size_bytes=size_bytes,
        digest=digest,
        state="verified",
        reason="verified",
    )
