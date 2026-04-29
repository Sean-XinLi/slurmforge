from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STARTER_EXAMPLE_START = "<!-- CONFIG_STARTER_EXAMPLE_START -->"
STARTER_EXAMPLE_END = "<!-- CONFIG_STARTER_EXAMPLE_END -->"
ADVANCED_EXAMPLE_START = "<!-- CONFIG_ADVANCED_EXAMPLE_START -->"
ADVANCED_EXAMPLE_END = "<!-- CONFIG_ADVANCED_EXAMPLE_END -->"
REFERENCE_START = "<!-- CONFIG_SCHEMA_REFERENCE_START -->"
REFERENCE_END = "<!-- CONFIG_SCHEMA_REFERENCE_END -->"
CONFIG_DOC = ROOT / "docs" / "config.md"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    sys.path.insert(0, str(ROOT / "src"))
    from slurmforge.config_schema import render_global_field_reference
    from slurmforge.starter.config_examples import (
        render_advanced_example,
        render_starter_example,
    )

    starter_example = render_starter_example(ROOT)
    advanced_example = render_advanced_example()
    expected_reference = render_global_field_reference()
    current = CONFIG_DOC.read_text(encoding="utf-8")
    rendered = _replace_between_markers(
        current,
        STARTER_EXAMPLE_START,
        STARTER_EXAMPLE_END,
        f"```yaml\n{starter_example.rstrip()}\n```",
    )
    rendered = _replace_between_markers(
        rendered,
        ADVANCED_EXAMPLE_START,
        ADVANCED_EXAMPLE_END,
        f"```yaml\n{advanced_example.rstrip()}\n```",
    )
    rendered = _replace_between_markers(
        rendered,
        REFERENCE_START,
        REFERENCE_END,
        expected_reference,
    )
    if args.check:
        if rendered != current:
            print(
                f"{CONFIG_DOC} is out of date; run tools/render_config_docs.py",
                file=sys.stderr,
            )
            return 1
        return 0
    CONFIG_DOC.write_text(rendered, encoding="utf-8")
    return 0


def _replace_between_markers(
    text: str, start_marker: str, end_marker: str, generated: str
) -> str:
    try:
        before, rest = text.split(start_marker, 1)
        _old, after = rest.split(end_marker, 1)
    except ValueError as exc:
        raise SystemExit(f"{CONFIG_DOC} is missing generated content markers") from exc
    return f"{before}{start_marker}\n{generated}\n{end_marker}{after}"


if __name__ == "__main__":
    raise SystemExit(main())
