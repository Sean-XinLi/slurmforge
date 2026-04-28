"""``sforge init`` -- scaffold starter experiment files."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

from ..errors import UsageError
from ..starter.defaults import DEFAULT_OUTPUT_DIR
from ..starter import (
    InitRequest,
    StarterWriteError,
    create_starter_project,
    template_choices,
    template_descriptions,
)
from ..starter.writers import existing_starter_files


def _is_interactive() -> bool:
    return sys.stdin.isatty()


def _prompt_template() -> str:
    choices = template_choices()
    print("Select template:")
    for index, (name, description) in enumerate(template_descriptions(), start=1):
        print(f"  {index}. {name} - {description}")
    while True:
        value = input("Template [1]: ").strip()
        if not value:
            return choices[0]
        if value.isdigit() and 1 <= int(value) <= len(choices):
            return choices[int(value) - 1]
        if value in choices:
            return value
        print(f"Invalid template: {value}")


def _prompt_output_dir() -> Path:
    value = input(f"Output project directory [{DEFAULT_OUTPUT_DIR}]: ").strip()
    return Path(value or DEFAULT_OUTPUT_DIR)


def _confirm_overwrite(paths: tuple[Path, ...]) -> bool:
    print("The following generated files already exist:")
    for path in paths:
        print(f"  {path}")
    answer = input("Overwrite? [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def _request_from_args(args: argparse.Namespace) -> InitRequest | None:
    if args.template is None:
        if not _is_interactive():
            raise UsageError(
                "sforge init requires --template when stdin is not interactive"
            )
        template = _prompt_template()
        output_dir = _prompt_output_dir()
    else:
        template = args.template
        output_dir = Path(args.output or DEFAULT_OUTPUT_DIR)
    request = InitRequest(template=template, output_dir=output_dir, force=args.force)
    existing = existing_starter_files(request)
    if existing and not request.force:
        if not _is_interactive():
            joined = ", ".join(str(path) for path in existing)
            raise StarterWriteError(
                f"Refusing to overwrite existing files: {joined}. Use --force to replace them."
            )
        if not _confirm_overwrite(existing):
            return None
        request = replace(request, force=True)
    return request


def handle_init(args: argparse.Namespace) -> None:
    if args.list_templates:
        for name, description in template_descriptions():
            print(f"{name}: {description}")
        return
    request = _request_from_args(args)
    if request is None:
        print("[INIT] cancelled")
        return
    result = create_starter_project(request)
    print(
        f"[INIT] template={result.template} output={result.output_dir} "
        f"config={result.config_path}"
    )
    for file in result.files:
        print(f"[INIT] wrote {file.role}: {file.path}")


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "init", help="Create a starter experiment config and scripts"
    )
    parser.add_argument(
        "--list-templates", action="store_true", help="List available starter templates"
    )
    parser.add_argument(
        "--template",
        choices=template_choices(),
        default=None,
        help="Starter template to generate",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=f"Project directory to write (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--force", action="store_true", help="Overwrite existing generated files"
    )
    parser.set_defaults(handler=handle_init)
