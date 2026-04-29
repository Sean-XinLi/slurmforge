from __future__ import annotations

from typing import Any

from ..config_contract.defaults import (
    DEFAULT_EMAIL_ENABLED,
    DEFAULT_EMAIL_EVENTS,
    DEFAULT_EMAIL_FROM,
    DEFAULT_EMAIL_MODE,
    DEFAULT_EMAIL_SENDMAIL,
    DEFAULT_EMAIL_SUBJECT_PREFIX,
)
from ..config_contract.options import options_for, options_sentence
from ..config_schema import reject_unknown_config_keys
from ..errors import ConfigContractError
from .models import EmailNotificationSpec, NotificationsSpec
from .parse_common import optional_mapping


def parse_email_recipients(raw: Any) -> tuple[str, ...]:
    if raw in (None, ""):
        return ()
    if isinstance(raw, str):
        return (raw,)
    if not isinstance(raw, list):
        raise ConfigContractError("`notifications.email.to` must be a string or list")
    return tuple(str(item) for item in raw if str(item))


def parse_notification_events(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return DEFAULT_EMAIL_EVENTS
    if not isinstance(raw, list):
        raise ConfigContractError("`notifications.email.on` must be a list")
    events = tuple(str(item) for item in raw)
    allowed = set(options_for("notifications.email.on"))
    invalid = sorted(set(events) - allowed)
    if invalid:
        joined = ", ".join(invalid)
        raise ConfigContractError(
            f"`notifications.email.on` contains unsupported events: {joined}"
        )
    return events


def parse_notifications(raw: Any) -> NotificationsSpec:
    data = optional_mapping(raw, "notifications")
    reject_unknown_config_keys(data, parent="notifications")
    email_data = optional_mapping(data.get("email"), "notifications.email")
    reject_unknown_config_keys(email_data, parent="notifications.email")
    mode = str(email_data.get("mode") or DEFAULT_EMAIL_MODE)
    if mode not in options_for("notifications.email.mode"):
        raise ConfigContractError(
            f"`notifications.email.mode` must be {options_sentence('notifications.email.mode')}"
        )
    return NotificationsSpec(
        email=EmailNotificationSpec(
            enabled=bool(email_data.get("enabled", DEFAULT_EMAIL_ENABLED)),
            to=parse_email_recipients(email_data.get("to")),
            events=parse_notification_events(email_data.get("on")),
            mode=mode,
            from_address=str(email_data.get("from") or DEFAULT_EMAIL_FROM),
            sendmail=str(email_data.get("sendmail") or DEFAULT_EMAIL_SENDMAIL),
            subject_prefix=str(
                email_data.get("subject_prefix") or DEFAULT_EMAIL_SUBJECT_PREFIX
            ),
        )
    )
