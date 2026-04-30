from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.registry import default_for

DEFAULT_EMAIL_ENABLED = default_for("notifications.email.enabled")
DEFAULT_EMAIL_FROM = default_for("notifications.email.from")
DEFAULT_EMAIL_MODE = default_for("notifications.email.mode")
DEFAULT_EMAIL_SENDMAIL = default_for("notifications.email.sendmail")
DEFAULT_EMAIL_SUBJECT_PREFIX = default_for("notifications.email.subject_prefix")


@dataclass(frozen=True)
class EmailNotificationSpec:
    enabled: bool = DEFAULT_EMAIL_ENABLED
    to: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    mode: str = DEFAULT_EMAIL_MODE
    from_address: str = DEFAULT_EMAIL_FROM
    sendmail: str = DEFAULT_EMAIL_SENDMAIL
    subject_prefix: str = DEFAULT_EMAIL_SUBJECT_PREFIX


@dataclass(frozen=True)
class NotificationsSpec:
    email: EmailNotificationSpec = field(default_factory=EmailNotificationSpec)
