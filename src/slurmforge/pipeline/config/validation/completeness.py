"""
Level 0 completeness validation: detect unfilled null sentinels and missing files.

This runs before the compiler pipeline and gives users actionable guidance
when required fields have not been configured.

Design: whitelist approach
--------------------------
Only paths listed in _REQUIRED_PATHS are checked for null.  A recursive
"find all nulls" scan would produce false positives for optional fields that
users legitimately leave as ~ (e.g. ``eval: ~``, ``model.yaml: ~``).  The
whitelist means new required fields must be registered here explicitly — a
small maintenance cost that eliminates confusing false-positive errors.

Public API
----------
check_completeness          → list[CompletenessIssue]
format_completeness_errors  → human-readable string
assert_complete             → hard gate; raises ValueError on any issue
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Union

from ....errors import ConfigContractError
from ._helpers import MISSING, get_nested
from .messages import format_completeness_report

__all__ = [
    "NullFieldIssue",
    "MissingFileIssue",
    "CompletenessIssue",
    "check_completeness",
    "format_completeness_errors",
    "assert_complete",
]


# ── data classes ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class NullFieldIssue:
    """A required field that is still null (unfilled sentinel)."""
    path: tuple[str, ...]
    config_path: Path


@dataclass(frozen=True)
class MissingFileIssue:
    """A referenced file that does not exist on disk."""
    path: tuple[str, ...]
    file_path: str
    config_path: Path


CompletenessIssue = Union[NullFieldIssue, MissingFileIssue]


# ── whitelist of required null sentinels ──────────────────────────────────────
#
# A path is included here iff the corresponding template uses ~ to mark it as
# "required, must be filled before generate can run".  Mode-specific paths
# (model.script, run.command, etc.) are present for all modes; the null check
# skips them when the path is simply absent from the config dict.

_REQUIRED_PATHS: frozenset[tuple[str, ...]] = frozenset({
    # Required for every template type
    ("cluster", "partition"),
    ("cluster", "account"),
    ("cluster", "time_limit"),
    # script / registry modes
    ("model", "script"),
    # command mode
    ("run", "command"),
    # registry mode
    ("model_registry", "registry_file"),
    # adapter mode
    ("run", "adapter", "script"),
})

# ── file-existence checks ─────────────────────────────────────────────────────
#
# Each entry: (field_label_path, cfg_traversal_path)
# Checked only when the field is present and non-null.

_FILE_FIELDS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (("model", "script"),                 ("model", "script")),
    (("model", "yaml"),                   ("model", "yaml")),
    (("model_registry", "registry_file"), ("model_registry", "registry_file")),
    (("run", "adapter", "script"),        ("run", "adapter", "script")),
    (("eval", "script"),                  ("eval", "script")),
)

# ── public API ────────────────────────────────────────────────────────────────

def check_completeness(
    cfg: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path | None = None,
) -> list[CompletenessIssue]:
    """
    Run Level 0 completeness checks on a raw config dict.

    Returns a (possibly empty) list of issues.  An empty list means the config
    is complete enough to pass to the compiler.
    """
    issues: list[CompletenessIssue] = []
    root = project_root or config_path.parent

    # ── null sentinel detection (whitelist only) ──────────────────────────────
    for path in sorted(_REQUIRED_PATHS):          # sorted for stable output order
        value = get_nested(cfg, *path, default=MISSING)
        if value is MISSING:
            continue  # field absent → optional or handled elsewhere
        if value is None:
            issues.append(NullFieldIssue(path=path, config_path=config_path))

    # ── file existence checks (skip null / absent fields) ─────────────────────
    for field_path, cfg_path in _FILE_FIELDS:
        value = get_nested(cfg, *cfg_path, default=MISSING)
        if value is MISSING or value is None:
            continue
        file_path = str(value).strip()
        if not file_path:
            continue
        if not (root / file_path).resolve().exists():
            issues.append(MissingFileIssue(
                path=field_path,
                file_path=file_path,
                config_path=config_path,
            ))

    return issues


def format_completeness_errors(
    issues: list[CompletenessIssue],
    *,
    config_path: Path,
) -> str:
    """Format completeness issues into a multi-line human-readable error string."""
    null_paths = [i.path for i in issues if isinstance(i, NullFieldIssue)]
    missing_files = [
        (i.path, i.file_path)
        for i in issues
        if isinstance(i, MissingFileIssue)
    ]
    return format_completeness_report(
        null_paths,
        missing_files,
        config_path=config_path,
    )


def assert_complete(
    cfg: dict[str, Any],
    *,
    config_path: Path,
    project_root: Path | None = None,
) -> None:
    """
    Hard gate: raise ValueError with a formatted message if the config has
    any completeness issues.  Call this before invoking the compiler.
    """
    issues = check_completeness(cfg, config_path=config_path, project_root=project_root)
    if issues:
        raise ConfigContractError(format_completeness_errors(issues, config_path=config_path))
