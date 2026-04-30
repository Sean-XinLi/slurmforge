from __future__ import annotations

from ...plans.notifications import (
    EmailNotificationPlan,
    FinalizerPlan,
    NotificationPlan,
)
from ...plans.runtime import RuntimePlan
from ...spec import ExperimentSpec
from .resources import control_resources_payload
from .runtime import environment_payload, executor_runtime_payload


def notification_payload(spec: ExperimentSpec) -> NotificationPlan:
    email = spec.notifications.email
    control = spec.orchestration.control
    return NotificationPlan(
        email=EmailNotificationPlan(
            enabled=email.enabled,
            to=email.to,
            events=email.events,
            mode=email.mode,
            from_address=email.from_address,
            sendmail=email.sendmail,
            subject_prefix=email.subject_prefix,
        ),
        finalizer=FinalizerPlan(
            resources=control_resources_payload(spec),
            environment_name=control.environment,
            environment_plan=environment_payload(spec, control.environment),
            runtime_plan=RuntimePlan(executor=executor_runtime_payload(spec)),
        ),
    )
