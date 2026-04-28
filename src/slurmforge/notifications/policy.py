from __future__ import annotations

from typing import Any


def email_notification_enabled(notification_plan: Any, event: str) -> bool:
    email = notification_plan.email
    return email.enabled and event in set(email.events)
