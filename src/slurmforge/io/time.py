from __future__ import annotations

import datetime


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
