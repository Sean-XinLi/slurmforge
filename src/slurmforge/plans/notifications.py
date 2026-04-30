from __future__ import annotations

from dataclasses import dataclass, field

from .resources import ControlResourcesPlan


@dataclass(frozen=True)
class EmailNotificationPlan:
    enabled: bool = False
    recipients: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    when: str = "afterany"
    mail_type: str = "END"


@dataclass(frozen=True)
class NotificationPlan:
    email: EmailNotificationPlan = field(default_factory=EmailNotificationPlan)
    resources: ControlResourcesPlan = field(default_factory=ControlResourcesPlan)
