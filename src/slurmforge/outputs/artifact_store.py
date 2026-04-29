from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..io import file_digest
from ..plans.outputs import ArtifactStorePlan
from .models import ArtifactRef


class ArtifactIntegrityError(RuntimeError):
    pass


def _managed_name(path: Path, digest: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", path.name)
    return f"{digest[:12]}_{safe}"


def _store_file(source: Path, managed: Path, *, strategy: str) -> tuple[str, bool]:
    if strategy == "register_only":
        return str(source), False
    managed.parent.mkdir(parents=True, exist_ok=True)
    if managed.exists() or managed.is_symlink():
        managed.unlink()
    if strategy == "copy":
        shutil.copy2(source, managed)
    elif strategy == "hardlink":
        os.link(source, managed)
    elif strategy == "symlink":
        managed.symlink_to(source)
    else:
        raise ConfigContractError(f"Unsupported artifact store strategy: {strategy}")
    return str(managed), True


def manage_file(
    path: str,
    *,
    attempt_dir: Path,
    kind: str,
    output_name: str | None = None,
    optional: bool = False,
    store_plan: ArtifactStorePlan,
) -> ArtifactRef:
    source = Path(path).resolve()
    source_digest = file_digest(source)
    strategy = store_plan.strategy
    fallback_strategy = store_plan.fallback_strategy
    verify_digest = store_plan.verify_digest
    fail_on_verify_error = store_plan.fail_on_verify_error
    files_dir = attempt_dir / "artifacts" / "files"
    managed = files_dir / _managed_name(source, source_digest)
    try:
        managed_path, is_managed = _store_file(source, managed, strategy=strategy)
        strategy_applied = strategy
    except OSError:
        if not fallback_strategy:
            raise
        managed_path, is_managed = _store_file(
            source, managed, strategy=str(fallback_strategy)
        )
        strategy_applied = str(fallback_strategy)
    managed_digest = source_digest
    verified = None
    verify_error = ""
    if verify_digest:
        try:
            managed_digest = file_digest(Path(managed_path))
            verified = managed_digest == source_digest
        except OSError as exc:
            verified = False
            verify_error = str(exc)
        if verified is False and fail_on_verify_error:
            detail = (
                verify_error
                or f"source_digest={source_digest} managed_digest={managed_digest}"
            )
            raise ArtifactIntegrityError(
                f"artifact digest verification failed for {managed_path}: {detail}"
            )
    return ArtifactRef(
        name=output_name or source.name,
        kind=kind,
        source_path=str(source),
        managed_path=managed_path,
        strategy=strategy_applied,
        managed=is_managed,
        digest=source_digest,
        source_digest=source_digest,
        managed_digest=managed_digest,
        verified=verified,
        size_bytes=source.stat().st_size,
        optional=optional,
    )


def artifact_payload(artifact: ArtifactRef) -> dict[str, Any]:
    return {
        "schema_version": artifact.schema_version,
        "name": artifact.name,
        "kind": artifact.kind,
        "source_path": artifact.source_path,
        "managed_path": artifact.managed_path,
        "strategy": artifact.strategy,
        "managed": artifact.managed,
        "digest": artifact.digest,
        "source_digest": artifact.source_digest,
        "managed_digest": artifact.managed_digest,
        "verified": artifact.verified,
        "size_bytes": artifact.size_bytes,
        "optional": artifact.optional,
    }
