"""
`sforge init` — create a starter project scaffold.

Decision tree (two orthogonal choices):

  TRAINING TYPE  (how is your training code invoked?)
    script    → train.py with CLI args            [most common]
    command   → complete shell command
    registry  → shared team model registry
    adapter   → interface bridge script

  PROFILE  (cluster complexity)
    starter   → single GPU, minimal config        [default]
    hpc       → multi-GPU, sweep, eval, artifact sync

Examples
--------
  sforge init                          # interactive wizard
  sforge init script                   # script · starter profile
  sforge init script --profile hpc    # script · hpc profile
  sforge init command
  sforge init command  --profile hpc
  sforge init registry
  sforge init registry --profile hpc
  sforge init adapter
  sforge init adapter  --profile hpc
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ..starter_catalog import PROFILES, TEMPLATE_TYPES, get_starter_spec
from ..starter_projects import init_project
from .init_wizard import run_wizard

_TYPE_DESCRIPTIONS = {
    "script":   "Scaffold for a train.py-style script — slurmforge manages args and submission.",
    "command":  "Scaffold that wraps a complete shell command in Slurm.",
    "registry": "Scaffold using a shared team model registry.",
    "adapter":  "Scaffold with an interface-bridge adapter script (advanced).",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_out_dir(template_type: str, profile: str) -> Path:
    return Path(f"./slurmforge_{template_type}_{profile}")


def _prompt_overwrite(out_dir: Path) -> bool:
    """
    Return True if it is safe to proceed (dir is empty/new, or user confirmed overwrite).
    Exits the process if the user declines or stdin is not a TTY.
    """
    if not out_dir.exists() or not any(out_dir.iterdir()):
        return True
    if not sys.stdin.isatty():
        print(
            f"[sforge init] '{out_dir}' is not empty. Re-run with --force to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"\n  '{out_dir.name}' already exists and is not empty.")
    try:
        answer = input("  Overwrite existing files? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    return answer in ("y", "yes")


# ---------------------------------------------------------------------------
# Shared argument builder
# ---------------------------------------------------------------------------

def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--profile",
        default="starter",
        choices=PROFILES,
        metavar="PROFILE",
        help=(
            "Cluster complexity profile. "
            "'starter' = single GPU, minimal config (default). "
            "'hpc' = multi-GPU, sweep, eval, artifact sync."
        ),
    )
    parser.add_argument(
        "--out",
        default=None,
        metavar="DIR",
        help=(
            "Destination directory for the project scaffold "
            "(default: ./slurmforge_<TYPE>_<PROFILE>)"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files without prompting",
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _do_init(*, template_type: str, profile: str, out: str | None, force: bool) -> None:
    out_dir = (
        Path(out) if out is not None else _default_out_dir(template_type, profile)
    ).expanduser().resolve()

    if not force:
        if not _prompt_overwrite(out_dir):
            print("[sforge init] Aborted.")
            return
        force = True  # user confirmed or dir was empty — propagate to init_project

    spec = get_starter_spec(template_type, profile)
    written = init_project(template_type, profile, out_dir, force=force)
    print(f"[OK] Initialized '{template_type}' scaffold (profile: {profile}) in: {out_dir}")
    print(f"[INFO] {spec.post_init_guidance}")
    print()
    print("  Files created:")
    for path in written:
        print(f"    {path}")
    print()
    print("  Next steps:")
    print(f"    1. Open  {out_dir / 'experiment.yaml'}")
    print("    2. Fill in every field marked with ~  (required — see STEP 1 comments)")
    print(f"    3. Run:  sforge validate --config {out_dir / 'experiment.yaml'}")
    print(f"    4. Run:  sforge generate --config {out_dir / 'experiment.yaml'}")


def handle_init_template(args: argparse.Namespace) -> None:
    _do_init(
        template_type=args.template_type,
        profile=args.profile,
        out=args.out,
        force=args.force,
    )


def handle_init_wizard(args: argparse.Namespace) -> None:
    """Fallback handler when no TYPE subcommand is given — launches interactive wizard."""
    template_type, profile = run_wizard()
    _do_init(template_type=template_type, profile=profile, out=args.out, force=args.force)


# ---------------------------------------------------------------------------
# Parser registration
# ---------------------------------------------------------------------------

def add_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    init_parser = subparsers.add_parser(
        "init",
        description=__doc__,
        help="Create a starter project scaffold  (run 'sforge init' for interactive setup)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Top-level --out/--force for wizard path (TYPE subcommand overrides these)
    init_parser.add_argument(
        "--out",
        default=None,
        metavar="DIR",
        help="Output directory (wizard mode — overridden by TYPE subcommand flags)",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files without prompting",
    )
    init_parser.set_defaults(handler=handle_init_wizard)

    # TYPE subcommands: script / command / registry / adapter
    type_subparsers = init_parser.add_subparsers(dest="template_type")

    for ttype in TEMPLATE_TYPES:
        tp = type_subparsers.add_parser(
            ttype,
            help=_TYPE_DESCRIPTIONS.get(ttype, ""),
            description=_TYPE_DESCRIPTIONS.get(ttype, ""),
        )
        tp.set_defaults(template_type=ttype, handler=handle_init_template)
        _add_common_args(tp)
