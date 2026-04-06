from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Iterable, Mapping

from .copier import copy_item, failure_record
from .discovery import collect_matches, normalize_workdirs
from .manifest import build_artifact_manifest


def sync_patterns(
    workdir: Path | str | Iterable[Path | str],
    result_dir: Path,
    patterns: list[str],
    category: str,
    max_matches_per_glob: int,
    *,
    warn_prefix: str = "artifact_sync",
) -> tuple[list[str], list[dict[str, str]]]:
    workdirs = normalize_workdirs(workdir)
    copied: list[str] = []
    failures: list[dict[str, str]] = []
    target = result_dir / category
    seen_sources: set[Path] = set()
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        failures.append(
            failure_record(
                category=category,
                pattern="*",
                stage="prepare_target",
                error=exc,
            )
        )
        print(f"[{warn_prefix}][WARN] failed to prepare target dir for category `{category}`: {exc}")
        return copied, failures

    for pattern in patterns:
        for source_root in workdirs:
            try:
                matches = collect_matches(
                    source_root,
                    pattern,
                    exclude_root=result_dir,
                    max_matches=max_matches_per_glob,
                    warn_prefix=warn_prefix,
                )
            except OSError as exc:
                failures.append(
                    failure_record(
                        category=category,
                        pattern=pattern,
                        stage="collect",
                        error=exc,
                    )
                )
                print(
                    f"[{warn_prefix}][WARN] failed to collect matches for pattern `{pattern}` "
                    f"under `{source_root}`: {exc}"
                )
                continue
            for src in matches:
                resolved_src = src.resolve()
                if resolved_src in seen_sources:
                    continue
                seen_sources.add(resolved_src)
                try:
                    dst = copy_item(src, target, source_root)
                except (OSError, shutil.Error) as exc:
                    failures.append(
                        failure_record(
                            category=category,
                            pattern=pattern,
                            stage="copy",
                            error=exc,
                            source=src,
                        )
                    )
                    print(f"[{warn_prefix}][WARN] failed to copy `{src}` for pattern `{pattern}`: {exc}")
                    continue
                if dst:
                    copied.append(dst)
    return copied, failures


def sync_artifacts(
    *,
    workdir: Path | str | Iterable[Path | str],
    result_dir: Path,
    category_patterns: Mapping[str, list[str]],
    max_matches_per_glob: int,
    warn_prefix: str = "artifact_sync",
) -> dict[str, object]:
    workdirs = normalize_workdirs(workdir)
    resolved_result_dir = result_dir.resolve()
    resolved_result_dir.mkdir(parents=True, exist_ok=True)

    copied: dict[str, list[str]] = {}
    failures: dict[str, list[dict[str, str]]] = {}
    for category, patterns in category_patterns.items():
        category_copied, category_failures = sync_patterns(
            workdir=workdirs,
            result_dir=resolved_result_dir,
            patterns=list(patterns or []),
            category=category,
            max_matches_per_glob=max_matches_per_glob,
            warn_prefix=warn_prefix,
        )
        copied[category] = category_copied
        failures[category] = category_failures

    manifest = build_artifact_manifest(
        workdirs=workdirs,
        result_dir=resolved_result_dir,
        copied=copied,
        failures=failures,
        max_matches_per_glob=max_matches_per_glob,
    )
    summary_path = resolved_result_dir / "meta" / "artifact_manifest.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest
