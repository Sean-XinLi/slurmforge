from __future__ import annotations

from dataclasses import dataclass, field

from ...config_contract.defaults import (
    DEFAULT_EMAIL_ENABLED,
    DEFAULT_EMAIL_FROM,
    DEFAULT_EMAIL_MODE,
    DEFAULT_EMAIL_SENDMAIL,
    DEFAULT_EMAIL_SUBJECT_PREFIX,
)


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
