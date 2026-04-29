from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
CONFIG_DOC = ROOT / "docs" / "config.md"
QUICKSTART_DOC = ROOT / "docs" / "quickstart.md"
SUBMISSION_DOC = ROOT / "docs" / "records" / "submission.md"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    sys.path.insert(0, str(SRC))
    from slurmforge.docs_render import (
        render_config_doc,
        render_quickstart_doc,
        render_submission_doc,
    )

    renderers: tuple[tuple[Path, Callable[[str], str]], ...] = (
        (
            CONFIG_DOC,
            lambda current: render_config_doc(
                current, path=CONFIG_DOC, project_root=ROOT
            ),
        ),
        (
            QUICKSTART_DOC,
            lambda current: render_quickstart_doc(current, path=QUICKSTART_DOC),
        ),
        (
            SUBMISSION_DOC,
            lambda current: render_submission_doc(current, path=SUBMISSION_DOC),
        ),
    )
    return _check_or_write(renderers, check=args.check)


def _check_or_write(
    renderers: tuple[tuple[Path, Callable[[str], str]], ...], *, check: bool
) -> int:
    for path, render in renderers:
        current = path.read_text(encoding="utf-8")
        rendered = render(current)
        if check and rendered != current:
            print(f"{path} is out of date; run tools/render_docs.py", file=sys.stderr)
            return 1
        if not check:
            path.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
