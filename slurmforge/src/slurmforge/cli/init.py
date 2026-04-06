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
from pathlib import Path

from ..starter_catalog import PROFILES, TEMPLATE_TYPES, get_starter_spec
from ..starter_projects import init_project
from .init_wizard import run_wizard

_DEFAULT_OUT = "./slurmforge_starter"

_TYPE_DESCRIPTIONS = {
    "script":   "Scaffold for a train.py-style script — slurmforge manages args and submission.",
    "command":  "Scaffold that wraps a complete shell command in Slurm.",
    "registry": "Scaffold using a shared team model registry.",
    "adapter":  "Scaffold with an interface-bridge adapter script (advanced).",
}


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
        default=_DEFAULT_OUT,
        metavar="DIR",
        help="Destination directory for the project scaffold (default: %(default)s)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files in the destination directory",
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _do_init(*, template_type: str, profile: str, out: str, force: bool) -> None:
    spec = get_starter_spec(template_type, profile)
    written = init_project(template_type, profile, Path(out), force=force)
    out_dir = Path(out).expanduser().resolve()
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
    template_type, profile, out = run_wizard(out=args.out, force=args.force)
    _do_init(template_type=template_type, profile=profile, out=out, force=args.force)


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
        default=_DEFAULT_OUT,
        metavar="DIR",
        help="Output directory (wizard mode — overridden by TYPE subcommand flags)",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files",
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
