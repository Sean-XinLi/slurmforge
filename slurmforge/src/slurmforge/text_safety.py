from __future__ import annotations

import re
import shlex


_SLURM_JOB_NAME_INVALID_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_DUPLICATE_SEPARATORS = re.compile(r"[_-]{2,}")


def shell_quote(value: object) -> str:
    return shlex.quote(str(value))


def slurm_safe_job_name(value: object, *, default: str = "job", max_length: int = 128) -> str:
    raw = str(value).strip()
    sanitized = _SLURM_JOB_NAME_INVALID_CHARS.sub("_", raw)
    sanitized = _DUPLICATE_SEPARATORS.sub("_", sanitized)
    sanitized = sanitized.strip("._-")
    if not sanitized:
        sanitized = default
    return sanitized[:max_length]
