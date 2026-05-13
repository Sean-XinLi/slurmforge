from __future__ import annotations

from typing import Any

from ...record_fields import (
    required_bool,
    required_object,
    required_string,
    required_string_array,
)
from ..notifications import EmailNotificationPlan, NotificationPlan
from .resources import control_resources_plan_from_dict


def notification_plan_from_dict(payload: dict[str, Any]) -> NotificationPlan:
    email = required_object(payload, "email", label="notification_plan")
    return NotificationPlan(
        email=EmailNotificationPlan(
            enabled=required_bool(email, "enabled", label="notification_plan.email"),
            recipients=required_string_array(
                email, "recipients", label="notification_plan.email"
            ),
            events=required_string_array(
                email, "events", label="notification_plan.email"
            ),
            when=required_string(
                email, "when", label="notification_plan.email", non_empty=True
            ),
            mail_type=required_string(
                email, "mail_type", label="notification_plan.email", non_empty=True
            ),
        ),
        resources=control_resources_plan_from_dict(
            required_object(payload, "resources", label="notification_plan")
        ),
    )
