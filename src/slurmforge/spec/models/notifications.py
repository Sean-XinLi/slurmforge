from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EmailNotificationSpec:
    enabled: bool = False
    to: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    mode: str = ""
    from_address: str = ""
    sendmail: str = ""
    subject_prefix: str = ""


@dataclass(frozen=True)
class NotificationsSpec:
    email: EmailNotificationSpec = field(default_factory=EmailNotificationSpec)
