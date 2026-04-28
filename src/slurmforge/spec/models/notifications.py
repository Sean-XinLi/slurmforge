from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EmailNotificationSpec:
    enabled: bool = False
    to: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    mode: str = "summary"
    from_address: str = "slurmforge@localhost"
    sendmail: str = "/usr/sbin/sendmail"
    subject_prefix: str = "SlurmForge"


@dataclass(frozen=True)
class NotificationsSpec:
    email: EmailNotificationSpec = field(default_factory=EmailNotificationSpec)
