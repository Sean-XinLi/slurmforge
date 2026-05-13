from __future__ import annotations

from ..plans.notifications import NotificationPlan


def email_notification_enabled(notification_plan: NotificationPlan, event: str) -> bool:
    email = notification_plan.email
    return email.enabled and bool(email.recipients) and event in set(email.events)
