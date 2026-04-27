from __future__ import annotations

from typing import Any

from ..errors import ConfigContractError
from .models import EmailNotificationSpec, NotificationsSpec
from .parse_common import optional_mapping, reject_unknown_keys


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
        return ()
    if not isinstance(raw, list):
        raise ConfigContractError("`notifications.email.on` must be a list")
    events = tuple(str(item) for item in raw)
    allowed = {"batch_finished", "train_eval_pipeline_finished"}
    invalid = sorted(set(events) - allowed)
    if invalid:
        joined = ", ".join(invalid)
        raise ConfigContractError(f"`notifications.email.on` contains unsupported events: {joined}")
    return events


def parse_notifications(raw: Any) -> NotificationsSpec:
    data = optional_mapping(raw, "notifications")
    reject_unknown_keys(data, allowed={"email"}, name="notifications")
    email_data = optional_mapping(data.get("email"), "notifications.email")
    reject_unknown_keys(
        email_data,
        allowed={"enabled", "to", "on", "mode", "from", "sendmail", "subject_prefix"},
        name="notifications.email",
    )
    mode = str(email_data.get("mode") or "summary")
    if mode != "summary":
        raise ConfigContractError("`notifications.email.mode` must be summary")
    return NotificationsSpec(
        email=EmailNotificationSpec(
            enabled=bool(email_data.get("enabled", False)),
            to=parse_email_recipients(email_data.get("to")),
            events=parse_notification_events(email_data.get("on")),
            mode=mode,
            from_address=str(email_data.get("from") or "slurmforge@localhost"),
            sendmail=str(email_data.get("sendmail") or "/usr/sbin/sendmail"),
            subject_prefix=str(email_data.get("subject_prefix") or "SlurmForge"),
        )
    )
