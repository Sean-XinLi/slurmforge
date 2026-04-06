from __future__ import annotations

from jinja2 import Environment, PackageLoader

from .text_safety import shell_quote


def build_template_env() -> Environment:
    env = Environment(
        loader=PackageLoader("slurmforge", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["shellquote"] = shell_quote
    return env
