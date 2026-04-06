"""
Interactive 2-question wizard for `sforge init` (no arguments).
Falls back gracefully when stdin is not a TTY (CI, piped input).
"""
from __future__ import annotations

import sys
from pathlib import Path


_TRAINING_TYPES = [
    ("script",   "I have a train.py — slurmforge manages my args and submission"),
    ("command",  "I have a complete launch command — just wrap it in Slurm"),
    ("registry", "My training code lives in a shared team model registry"),
    ("adapter",  "I need an interface bridge between slurmforge and my training code"),
]

_PROFILES = [
    ("starter", "Quick start — single GPU, minimal config (recommended for first run)"),
    ("hpc",     "Full HPC — multi-GPU, sweep, eval, artifact sync"),
]


def _prompt_choice(
    prompt: str,
    options: list[tuple[str, str]],
) -> str:
    """Display numbered options and return the selected key."""
    print(prompt)
    for i, (key, description) in enumerate(options, 1):
        print(f"  {i}) {key:<12} {description}")
    print()
    while True:
        try:
            raw = input("  Enter number: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                chosen_key = options[idx][0]
                print(f"  → {chosen_key}")
                print()
                return chosen_key
        print(f"  Please enter a number between 1 and {len(options)}.")


def run_wizard(*, out: str, force: bool) -> tuple[str, str, str]:
    """
    Run the interactive wizard and return (template_type, profile, out_dir).
    Raises SystemExit if the user cancels.
    """
    if not sys.stdin.isatty():
        print(
            "[sforge init] No template type specified.\n"
            "Usage: sforge init <TYPE> [--profile starter|hpc] [--out DIR]\n"
            "\n"
            "Available types: script, command, registry, adapter\n"
            "Run 'sforge init --help' for full usage.",
            file=sys.stderr,
        )
        sys.exit(1)

    print()
    print("  ┌──────────────────────────────────────────────────────┐")
    print("  │  sforge init · project setup wizard                  │")
    print("  └──────────────────────────────────────────────────────┘")
    print()

    template_type = _prompt_choice(
        "  How is your training code invoked?",
        _TRAINING_TYPES,
    )

    profile = _prompt_choice(
        "  Which cluster profile fits your setup?",
        _PROFILES,
    )

    return template_type, profile, out
