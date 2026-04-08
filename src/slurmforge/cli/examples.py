"""``sforge examples`` -- list, show, or export shipped YAML reference examples."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..example_configs import export_example, get_example_list_view, read_example_text

_NAME_WIDTH = 23  # column width for the example name field


def handle_examples_list(_args: argparse.Namespace) -> None:
    view = get_example_list_view()
    qs_name, qs_desc = view["quick_start"]  # type: ignore[misc]

    print("Quick start:")
    print(f"  {qs_name:<{_NAME_WIDTH}} {qs_desc}")
    print()
    print("\u2501" * 32)

    for heading, entries in view["groups"]:  # type: ignore[misc]
        print()
        print(heading)
        for name, desc in entries:
            if desc:
                print(f"  {name:<{_NAME_WIDTH}} {desc}")
            else:
                print(f"  {name}")
    print()


def handle_examples_show(args: argparse.Namespace) -> None:
    print(read_example_text(args.name), end="")


def handle_examples_export(args: argparse.Namespace) -> None:
    exported = export_example(args.name, Path(args.out), force=args.force)
    print(f"[OK] Wrote example config: {exported}")


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    examples_parser = subparsers.add_parser(
        "examples",
        help="List, show, or export shipped raw YAML reference examples",
    )
    examples_subparsers = examples_parser.add_subparsers(dest="examples_command")
    examples_subparsers.required = True

    list_parser = examples_subparsers.add_parser("list", help="List shipped raw YAML reference examples")
    list_parser.set_defaults(handler=handle_examples_list)

    show_parser = examples_subparsers.add_parser("show", help="Print one shipped raw YAML reference example")
    show_parser.add_argument("name", help="Example name, with or without .yaml suffix")
    show_parser.set_defaults(handler=handle_examples_show)

    export_parser = examples_subparsers.add_parser(
        "export",
        help="Copy one shipped raw YAML reference example to a file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  sforge examples export command_starter --out ./experiment.yaml\n"
            "  sforge examples export script_hpc --out ./experiment.yaml --force\n"
        ),
    )
    export_parser.add_argument("name", help="Example name, with or without .yaml suffix")
    export_parser.add_argument("--out", required=True, metavar="FILE", help="Destination path for the exported YAML (e.g. ./experiment.yaml)")
    export_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the destination file if it already exists",
    )
    export_parser.set_defaults(handler=handle_examples_export)
