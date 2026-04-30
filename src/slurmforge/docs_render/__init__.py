from __future__ import annotations

__all__ = ["render_config_doc", "render_quickstart_doc", "render_submission_doc"]


def __getattr__(name: str) -> object:
    if name == "render_config_doc":
        from .config_doc import render_config_doc

        return render_config_doc
    if name == "render_quickstart_doc":
        from .quickstart import render_quickstart_doc

        return render_quickstart_doc
    if name == "render_submission_doc":
        from .submission import render_submission_doc

        return render_submission_doc
    raise AttributeError(name)
