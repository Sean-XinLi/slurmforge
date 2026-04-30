from __future__ import annotations

from typing import Any

from ..notifications import EmailNotificationPlan, NotificationPlan
from .resources import control_resources_plan_from_dict


def notification_plan_from_dict(payload: dict[str, Any]) -> NotificationPlan:
    email = dict(payload["email"])
    return NotificationPlan(
        email=EmailNotificationPlan(
            enabled=bool(email["enabled"]),
            recipients=tuple(str(item) for item in email["recipients"]),
            events=tuple(str(item) for item in email["events"]),
            when=str(email["when"]),
            mail_type=str(email["mail_type"]),
        ),
        resources=control_resources_plan_from_dict(payload["resources"]),
    )
