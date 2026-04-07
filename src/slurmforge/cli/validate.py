"""``sforge validate`` -- validate an experiment config without generating a batch."""
from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from ..pipeline.compiler import AuthoringSourceRequest, BatchCompileError, compile_source, iter_compile_report_lines
from ..pipeline.compiler.reports import report_has_failures, report_total_runs, report_warning_count
from ..pipeline.config.validation.advisory import check_advisory
from ..pipeline.config.validation.completeness import check_completeness, format_completeness_errors
from ..pipeline.config.validation.correctness import check_correctness
from ..pipeline.config.validation.messages import format_advisory_report, format_correctness_report
from .common import add_config_override_args, load_effective_cfg


def render_validate(
    *,
    config_path: Path,
    cli_overrides: list[str],
    project_root_override: str | None,
    force: bool,
) -> None:
    resolved_config_path = config_path.resolve()
    project_root = (
        Path(project_root_override).resolve()
        if project_root_override is not None
        else resolved_config_path.parent
    )

    effective_cfg = load_effective_cfg(resolved_config_path, cli_overrides)

    # ── Level 0: completeness (reported; does not abort validate) ────────────
    completeness_issues = check_completeness(
        effective_cfg, config_path=resolved_config_path, project_root=project_root
    )
    if completeness_issues:
        print(format_completeness_errors(completeness_issues, config_path=resolved_config_path))
        print()
        print("[NOTE] Fix the required fields above before running sforge generate.")
        print()

    # ── Level 1: correctness (hard error; --force skips) ─────────────────────
    correctness_errors = check_correctness(effective_cfg, config_path=resolved_config_path)
    if correctness_errors:
        print(format_correctness_report(
            correctness_errors,
            config_path=resolved_config_path,
            force_flag_available=True,
        ))
        print()
        if not force:
            raise SystemExit(1)
        print("[WARN] --force: skipping correctness errors.")
        print()

    # ── Level 2: advisory (warnings; never blocks) ───────────────────────────
    advisory_warnings = check_advisory(effective_cfg, config_path=resolved_config_path)
    if advisory_warnings:
        print(format_advisory_report(advisory_warnings, config_path=resolved_config_path))
        print()

    # ── Compiler pipeline (full structural + semantic validation) ─────────────
    report = compile_source(
        AuthoringSourceRequest(
            config_path=resolved_config_path,
            cli_overrides=tuple(cli_overrides),
            project_root=None if project_root_override is None else Path(project_root_override),
            default_batch_name=datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f"),
        ),
    )
    for line in iter_compile_report_lines(report):
        print(line)
    if report_has_failures(report):
        raise BatchCompileError(report)
    assert report.identity is not None
    print(f"[OK] Config is valid: {resolved_config_path}")
    print(f"[OK] project_root={report.identity.project_root}")
    print(f"[OK] planned_runs={report_total_runs(report)}")
    print(f"[OK] planning_warnings={report_warning_count(report)}")


def handle_validate(args: argparse.Namespace) -> None:
    render_validate(
        config_path=Path(args.config),
        cli_overrides=args.set,
        project_root_override=args.project_root,
        force=args.force,
    )


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate an experiment config without generating a batch",
    )
    validate_parser.add_argument(
        "--config",
        required=True,
        help="Path to experiment config yaml",
    )
    validate_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip Level 1 correctness errors (not recommended)",
    )
    add_config_override_args(validate_parser)
    validate_parser.set_defaults(handler=handle_validate)
