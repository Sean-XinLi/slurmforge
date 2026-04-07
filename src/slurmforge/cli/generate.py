from __future__ import annotations
"""``sforge generate`` -- expand an experiment config into sbatch arrays."""

import argparse
import datetime
from pathlib import Path

from ..pipeline.compiler import AuthoringSourceRequest, BatchCompileError, compile_source, iter_compile_report_lines
from ..pipeline.compiler.reports import require_success
from ..pipeline.config.validation.advisory import check_advisory
from ..pipeline.config.validation.completeness import assert_complete
from ..pipeline.config.validation.correctness import check_correctness
from ..pipeline.config.validation.messages import format_advisory_report, format_correctness_report
from .common import (
    add_common_args,
    load_effective_cfg,
    materialize_or_print_batch,
    print_batch_ready,
)


def render_generate(
    *,
    config_path: Path,
    cli_overrides: list[str],
    dry_run: bool,
    project_root_override: str | None,
) -> None:
    resolved_config_path = config_path.resolve()
    project_root = (
        Path(project_root_override).resolve()
        if project_root_override is not None
        else resolved_config_path.parent
    )

    effective_cfg = load_effective_cfg(resolved_config_path, cli_overrides)

    # ── Level 0: completeness gate (hard — null sentinels block generation) ──
    assert_complete(effective_cfg, config_path=resolved_config_path, project_root=project_root)

    # ── Level 1: correctness gate (hard — format/logic errors block generation) ─
    correctness_errors = check_correctness(effective_cfg, config_path=resolved_config_path)
    if correctness_errors:
        print(format_correctness_report(
            correctness_errors,
            config_path=resolved_config_path,
            force_flag_available=False,
        ))
        raise SystemExit(1)

    # ── Level 2: advisory (soft — warnings printed, generation continues) ────
    advisory_warnings = check_advisory(effective_cfg, config_path=resolved_config_path)
    if advisory_warnings:
        print(format_advisory_report(advisory_warnings, config_path=resolved_config_path))
        print()

    # ── Compiler pipeline ─────────────────────────────────────────────────────
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
    try:
        planned_batch = require_success(report)
    except BatchCompileError:
        raise

    dispatch = materialize_or_print_batch(
        planned_batch=planned_batch,
        dry_run=dry_run,
    )
    if dispatch is None:
        return
    print_batch_ready(dispatch=dispatch, sbatch_dir=planned_batch.sbatch_dir)


def handle_generate(args: argparse.Namespace) -> None:
    render_generate(
        config_path=Path(args.config),
        cli_overrides=args.set,
        dry_run=args.dry_run,
        project_root_override=args.project_root,
    )


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    generate_parser = subparsers.add_parser(
        "generate",
        help="Expand config into a new batch and render sbatch arrays",
    )
    generate_parser.add_argument(
        "--config",
        required=True,
        help="Path to experiment config yaml",
    )
    add_common_args(generate_parser)
    generate_parser.set_defaults(handler=handle_generate)
