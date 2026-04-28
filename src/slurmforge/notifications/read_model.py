from __future__ import annotations

from pathlib import Path
from typing import Any

from ..root_model.notifications import load_notification_summary_input as _load_notification_summary_input
from ..root_model.notifications import notification_plan_for_root as _notification_plan_for_root
from .models import NotificationSummaryInput


def notification_plan_for_root(root: Path) -> Any:
    return _notification_plan_for_root(root)


def load_notification_summary_input(root: Path, *, event: str) -> NotificationSummaryInput:
    return _load_notification_summary_input(root, event=event)
