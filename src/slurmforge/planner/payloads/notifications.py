from __future__ import annotations

from ...plans.notifications import (
    EmailNotificationPlan,
    NotificationPlan,
)
from ...spec import ExperimentSpec
from .resources import control_resources_payload


def notification_payload(spec: ExperimentSpec) -> NotificationPlan:
    email = spec.notifications.email
    return NotificationPlan(
        email=EmailNotificationPlan(
            enabled=email.enabled,
            recipients=email.recipients,
            events=email.events,
            when=email.when,
        ),
        resources=control_resources_payload(spec),
    )
