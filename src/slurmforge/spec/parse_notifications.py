from __future__ import annotations

from typing import Any

from ..config_contract.keys import reject_unknown_config_keys
from ..config_contract.registry import default_for, options_for, options_sentence
from ..errors import ConfigContractError
from .models import EmailNotificationSpec, NotificationsSpec
from .parse_common import optional_mapping


def parse_email_recipients(raw: Any) -> tuple[str, ...]:
    if raw in (None, ""):
        return ()
    if isinstance(raw, str):
        return (raw,)
    if not isinstance(raw, list):
        raise ConfigContractError(
            "`notifications.email.recipients` must be a string or list"
        )
    return tuple(str(item) for item in raw if str(item))


def parse_notification_events(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return default_for("notifications.email.events")
    if not isinstance(raw, list):
        raise ConfigContractError("`notifications.email.events` must be a list")
    events = tuple(str(item) for item in raw)
    allowed = set(options_for("notifications.email.events"))
    invalid = sorted(set(events) - allowed)
    if invalid:
        joined = ", ".join(invalid)
        raise ConfigContractError(
            f"`notifications.email.events` contains unsupported events: {joined}"
        )
    return events


def parse_notification_when(raw: Any) -> str:
    when = str(raw or default_for("notifications.email.when"))
    if when not in options_for("notifications.email.when"):
        raise ConfigContractError(
            f"`notifications.email.when` must be {options_sentence('notifications.email.when')}"
        )
    return when


def parse_notifications(raw: Any) -> NotificationsSpec:
    data = optional_mapping(raw, "notifications")
    reject_unknown_config_keys(data, parent="notifications")
    email_data = optional_mapping(data.get("email"), "notifications.email")
    reject_unknown_config_keys(email_data, parent="notifications.email")
    return NotificationsSpec(
        email=EmailNotificationSpec(
            enabled=bool(
                email_data.get("enabled", default_for("notifications.email.enabled"))
            ),
            recipients=parse_email_recipients(email_data.get("recipients")),
            events=parse_notification_events(email_data.get("events")),
            when=parse_notification_when(email_data.get("when")),
        )
    )
