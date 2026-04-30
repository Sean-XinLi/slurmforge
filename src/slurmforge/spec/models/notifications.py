from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EmailNotificationSpec:
    enabled: bool = False
    recipients: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    when: str = ""


@dataclass(frozen=True)
class NotificationsSpec:
    email: EmailNotificationSpec = field(default_factory=EmailNotificationSpec)
