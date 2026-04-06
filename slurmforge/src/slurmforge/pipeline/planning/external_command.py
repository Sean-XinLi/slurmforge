from __future__ import annotations

import shlex

from ...errors import PlanningError


def resolve_external_command_text(
    raw_command: str,
    *,
    command_mode: str | None,
    command_field_name: str,
    mode_field_name: str,
    resume_from_checkpoint: str | None = None,
) -> tuple[str, str]:
    command_text = str(raw_command or "").strip()
    if not command_text:
        raise PlanningError(f"{command_field_name} is empty")

    normalized_mode = str(command_mode or "argv").strip().lower()
    if normalized_mode not in {"argv", "raw"}:
        raise PlanningError(f"{mode_field_name} must be one of: argv, raw")

    if normalized_mode == "raw":
        return command_text, normalized_mode

    argv = shlex.split(command_text)
    if not argv:
        raise PlanningError(f"{command_field_name} must contain at least one executable token")

    checkpoint = str(resume_from_checkpoint or "").strip()
    if checkpoint and not any(token.startswith("--resume_from_checkpoint") for token in argv):
        argv.extend(["--resume_from_checkpoint", checkpoint])
    return shlex.join(argv), normalized_mode
