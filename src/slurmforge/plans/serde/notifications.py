from __future__ import annotations

from typing import Any

from ..notifications import EmailNotificationPlan, FinalizerPlan, NotificationPlan
from .resources import control_resources_plan_from_dict
from .runtime import environment_plan_from_dict, runtime_plan_from_dict


def notification_plan_from_dict(payload: dict[str, Any]) -> NotificationPlan:
    email = dict(payload["email"])
    finalizer = dict(payload["finalizer"])
    return NotificationPlan(
        email=EmailNotificationPlan(
            enabled=bool(email["enabled"]),
            to=tuple(str(item) for item in email["to"]),
            events=tuple(str(item) for item in email["events"]),
            mode=str(email["mode"]),
            from_address=str(email["from_address"]),
            sendmail=str(email["sendmail"]),
            subject_prefix=str(email["subject_prefix"]),
        ),
        finalizer=FinalizerPlan(
            resources=control_resources_plan_from_dict(finalizer["resources"]),
            environment_name=str(finalizer["environment_name"]),
            environment_plan=environment_plan_from_dict(finalizer["environment_plan"]),
            runtime_plan=runtime_plan_from_dict(finalizer["runtime_plan"]),
        ),
    )
