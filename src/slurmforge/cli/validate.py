"""``sforge validate`` -- validate a stage-batch experiment spec."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..spec import expand_run_definitions, load_experiment_spec


def render_validate(
    *,
    config_path: Path,
    cli_overrides: list[str],
    project_root_override: str | None,
    force: bool = False,
) -> None:
    del force
    spec = load_experiment_spec(
        config_path,
        cli_overrides=tuple(cli_overrides),
        project_root=None if project_root_override is None else Path(project_root_override),
    )
    runs = expand_run_definitions(spec)
    print(f"[OK] Config is valid: {spec.config_path}")
    print(f"[OK] project={spec.project} experiment={spec.experiment}")
    print(f"[OK] stages={' -> '.join(spec.stage_order())}")
    print(f"[OK] planned_runs={len(runs)}")
    print(f"[OK] spec_snapshot_digest={spec.spec_snapshot_digest}")


def handle_validate(args: argparse.Namespace) -> None:
    render_validate(
        config_path=Path(args.config),
        cli_overrides=args.set,
        project_root_override=args.project_root,
        force=args.force,
    )


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("validate", help="Validate a stage-batch experiment spec without emitting files")
    parser.add_argument("--config", required=True, help="Path to experiment config yaml")
    parser.add_argument("--set", action="append", default=[], help="Override config by dot-path")
    parser.add_argument("--project_root", default=None, help="Override project root for relative paths")
    parser.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
    parser.set_defaults(handler=handle_validate)
