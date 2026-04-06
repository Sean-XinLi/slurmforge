from __future__ import annotations

from .codecs import (
    serialize_artifacts_config,
    serialize_cluster_config,
    serialize_env_config,
    serialize_launcher_config,
    serialize_notify_config,
    serialize_resources_config,
    serialize_validation_config,
)
from .models import ArtifactsConfig, ClusterConfig, EnvConfig, LauncherConfig, NotifyConfig, ResourcesConfig, ValidationConfig

DEFAULT_CLUSTER = serialize_cluster_config(ClusterConfig())
DEFAULT_ENV = serialize_env_config(EnvConfig())
DEFAULT_LAUNCHER = serialize_launcher_config(LauncherConfig())
DEFAULT_RESOURCES = serialize_resources_config(ResourcesConfig())
DEFAULT_ARTIFACTS = serialize_artifacts_config(ArtifactsConfig())
DEFAULT_NOTIFY = serialize_notify_config(NotifyConfig())
DEFAULT_VALIDATION = serialize_validation_config(ValidationConfig())

__all__ = [
    "DEFAULT_ARTIFACTS",
    "DEFAULT_CLUSTER",
    "DEFAULT_ENV",
    "DEFAULT_LAUNCHER",
    "DEFAULT_NOTIFY",
    "DEFAULT_RESOURCES",
    "DEFAULT_VALIDATION",
]
