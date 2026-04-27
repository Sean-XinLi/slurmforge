from __future__ import annotations

from dataclasses import dataclass, field

from .resources import ControlResourcesPlan
from .runtime import EnvironmentPlan, RuntimePlan


@dataclass(frozen=True)
class EmailNotificationPlan:
    enabled: bool = False
    to: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    mode: str = "summary"
    from_address: str = "slurmforge@localhost"
    sendmail: str = "/usr/sbin/sendmail"
    subject_prefix: str = "SlurmForge"


@dataclass(frozen=True)
class FinalizerPlan:
    resources: ControlResourcesPlan = field(default_factory=ControlResourcesPlan)
    environment_name: str = ""
    environment_plan: EnvironmentPlan = field(default_factory=EnvironmentPlan)
    runtime_plan: RuntimePlan | None = None


@dataclass(frozen=True)
class NotificationPlan:
    email: EmailNotificationPlan = field(default_factory=EmailNotificationPlan)
    finalizer: FinalizerPlan = field(default_factory=FinalizerPlan)
