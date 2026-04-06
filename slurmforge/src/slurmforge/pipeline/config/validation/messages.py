"""
Centralized message templates for all user-facing validation output.

All formatting lives here so validation logic stays pure and message copy
can be updated without touching detection code.

Three levels, three formatters:
  format_completeness_report → Level 0: null sentinels + missing files
  format_correctness_report  → Level 1: format / logic errors
  format_advisory_report     → Level 2: best-practice warnings
"""
from __future__ import annotations

from pathlib import Path

__all__ = [
    "FIELD_GUIDES",
    "format_completeness_report",
    "format_correctness_report",
    "format_advisory_report",
]


# ── Level 0: field guides ─────────────────────────────────────────────────────

FIELD_GUIDES: dict[tuple[str, ...], dict[str, str]] = {
    ("cluster", "partition"): {
        "what": "GPU partition name on your cluster",
        "example": '"gpu", "a100", "v100", "gpu-h100"',
        "how": 'sinfo -o "%P %G" | grep -i gpu',
    },
    ("cluster", "account"): {
        "what": "your cluster account or project name",
        "example": '"my_lab", "project_abc"',
        "how": "sacctmgr show user $(whoami) format=user,account",
    },
    ("cluster", "time_limit"): {
        "what": "maximum wall-clock time for the job",
        "example": '"02:00:00" (2 h)  or  "1-12:00:00" (1 day 12 h)',
        "how": "estimate training time and add ~20% buffer",
    },
    ("model", "script"): {
        "what": "path to your training entry-point script",
        "example": '"src/train.py", "scripts/finetune.py"',
        "how": "the Python file that accepts --lr, --epochs, etc. as CLI args",
    },
    ("run", "command"): {
        "what": "the full shell command to launch training",
        "example": '"python train.py --epochs 10 --lr 1e-4"',
        "how": "write the exact command you would run manually on a GPU node",
    },
    ("model_registry", "registry_file"): {
        "what": "path to your models.yaml registry file",
        "example": '"models.yaml", "config/registry.yaml"',
        "how": "this file defines available models; see the generated models.yaml",
    },
    ("run", "adapter", "script"): {
        "what": "path to your adapter bridge script",
        "example": '"train_adapter.py", "src/adapter.py"',
        "how": "this script bridges slurmforge's interface to your training code",
    },
}

_GENERIC_FIELD_GUIDE = {
    "what": "a required configuration value",
    "example": "see field description in your config file",
    "how": "replace ~ with the appropriate value for your setup",
}


def _field_guide(path: tuple[str, ...]) -> dict[str, str]:
    return FIELD_GUIDES.get(path, _GENERIC_FIELD_GUIDE)


# ── Level 0: completeness ─────────────────────────────────────────────────────

def format_completeness_report(
    null_paths: list[tuple[str, ...]],
    missing_files: list[tuple[tuple[str, ...], str]],
    *,
    config_path: Path,
) -> str:
    """
    Build a human-readable completeness error block.

    Args:
        null_paths:    List of dot-path tuples for every unfilled ~ field.
        missing_files: List of (field_path, file_path_str) for missing files.
        config_path:   Path to the config file (shown in the header).
    """
    lines: list[str] = []
    lines.append(f"✗  Config is not ready to generate: {config_path}")
    lines.append("")

    if null_paths:
        n = len(null_paths)
        lines.append(
            f"   {n} required field{'s' if n != 1 else ''} not yet configured"
            " (replace ~ with a real value):"
        )
        lines.append("")
        for path in null_paths:
            guide = _field_guide(path)
            label = ".".join(path)
            lines.append(f"   {label}")
            lines.append(f"     what    : {guide['what']}")
            lines.append(f"     example : {guide['example']}")
            lines.append(f"     how     : {guide['how']}")
            lines.append("")

    if missing_files:
        n = len(missing_files)
        lines.append(
            f"   {n} referenced file{'s' if n != 1 else ''} not found on disk:"
        )
        lines.append("")
        for path, file_path in missing_files:
            label = ".".join(path)
            lines.append(f"   {label} = {file_path!r}")
            lines.append("     File does not exist relative to the config directory.")
            lines.append("     Check the path and ensure the file has been created.")
            lines.append("")

    lines.append(
        "   Fix the items above, then re-run:  sforge generate --config "
        + str(config_path)
    )
    return "\n".join(lines)


# ── Level 1: correctness ──────────────────────────────────────────────────────

def format_correctness_report(
    errors: list[tuple[str, str]],
    *,
    config_path: Path,
    force_flag_available: bool = True,
) -> str:
    """
    Build a human-readable correctness error block.

    Args:
        errors:               List of (field_label, message) pairs.
        config_path:          Path to the config file.
        force_flag_available: If True, append a note about --force.
    """
    lines: list[str] = []
    n = len(errors)
    lines.append(
        f"✗  {n} correctness error{'s' if n != 1 else ''} in: {config_path}"
    )
    lines.append("")
    for label, message in errors:
        lines.append(f"   {label}")
        lines.append(f"     {message}")
        lines.append("")
    if force_flag_available:
        lines.append(
            "   Use --force with sforge validate to skip correctness checks (not recommended)."
        )
    return "\n".join(lines)


# ── Level 2: advisory ─────────────────────────────────────────────────────────

def format_advisory_report(
    warnings: list[tuple[str, str]],
    *,
    config_path: Path,
) -> str:
    """
    Build a human-readable advisory warning block.

    Args:
        warnings:    List of (field_label, message) pairs.
        config_path: Path to the config file.
    """
    lines: list[str] = []
    n = len(warnings)
    lines.append(
        f"⚠  {n} advisory warning{'s' if n != 1 else ''} for: {config_path}"
    )
    lines.append("")
    for label, message in warnings:
        lines.append(f"   {label}")
        lines.append(f"     {message}")
        lines.append("")
    return "\n".join(lines)
