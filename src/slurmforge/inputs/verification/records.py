from __future__ import annotations

from ...contracts import (
    InputBinding,
    binding_is_ready_for_injection,
    resolved_kind_matches_expectation,
    resolved_payload_present,
)
from ...io import to_jsonable
from ..models import InputVerificationRecord
from .digests import expected_digest as expected_digest_for_binding
from .digests import producer_digest, value_digest
from .path_checks import verify_resolved_path


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
        "producer_digest": producer_digest(binding),
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
    expected_digest = expected_digest_for_binding(binding)
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
        return _record_for_value(binding, base=base, expected_digest=expected_digest)
    return _record_for_path(binding, base=base, expected_digest=expected_digest, required=required)


def _record_for_value(
    binding: InputBinding,
    *,
    base: dict[str, object],
    expected_digest: str,
) -> InputVerificationRecord:
    digest = value_digest(binding.resolved.value)
    if expected_digest and digest != expected_digest:
        return InputVerificationRecord(
            **base,
            path_kind="value",
            value_digest=digest,
            state="failed",
            failure_class="input_contract_error",
            reason=f"input value digest mismatch: expected {expected_digest}, got {digest}",
        )
    return InputVerificationRecord(
        **base,
        path_kind="value",
        value_digest=digest,
        state="verified",
        reason="verified value",
    )


def _record_for_path(
    binding: InputBinding,
    *,
    base: dict[str, object],
    expected_digest: str,
    required: bool,
) -> InputVerificationRecord:
    result = verify_resolved_path(binding, expected_digest=expected_digest)
    if not result.exists:
        state = "failed" if required else "skipped"
        return InputVerificationRecord(
            **base,
            path_kind=result.path_kind,
            exists=False,
            readable=False,
            size_bytes=None,
            state=state,
            failure_class="input_contract_error" if required else None,
            reason=f"input path does not exist: {result.path}",
        )
    if not result.is_valid_kind:
        state = "failed" if required else "skipped"
        return InputVerificationRecord(
            **base,
            path_kind=result.path_kind,
            exists=True,
            readable=False,
            size_bytes=None,
            state=state,
            failure_class="input_contract_error" if required else None,
            reason=f"input path is not a {result.path_kind}: {result.path}",
        )
    if not result.readable:
        state = "failed" if required else "skipped"
        return InputVerificationRecord(
            **base,
            path_kind=result.path_kind,
            exists=True,
            readable=False,
            size_bytes=result.size_bytes,
            state=state,
            failure_class="input_contract_error" if required else None,
            reason=f"input path is not readable: {result.path}",
        )
    if expected_digest and result.digest != expected_digest:
        return InputVerificationRecord(
            **base,
            path_kind=result.path_kind,
            exists=True,
            readable=True,
            size_bytes=result.size_bytes,
            digest=result.digest,
            state="failed",
            failure_class="input_contract_error",
            reason=f"input digest mismatch: expected {expected_digest}, got {result.digest}",
        )
    return InputVerificationRecord(
        **base,
        path_kind=result.path_kind,
        exists=True,
        readable=True,
        size_bytes=result.size_bytes,
        digest=result.digest,
        state="verified",
        reason="verified",
    )
