from __future__ import annotations

from dataclasses import dataclass

from ...models import (
    BatchSharedSpec,
    EvalConfigSpec,
    ModelConfigSpec,
    OutputConfigSpec,
    RunConfigSpec,
    StorageConfigSpec,
)
from ...normalize import (
    normalize_artifacts,
    normalize_cluster,
    normalize_dispatch,
    normalize_env,
    normalize_launcher,
    normalize_notify,
    normalize_resources,
    normalize_validation,
)
from ...runtime import (
    ArtifactsConfig,
    ClusterConfig,
    DispatchConfig,
    EnvConfig,
    LauncherConfig,
    NotifyConfig,
    ResourcesConfig,
    ValidationConfig,
)
from ..eval import normalize_eval_config
from ..output import normalize_output_config
from ..run import build_run_spec, normalize_model_config, validate_external_command_launcher
from ..storage import normalize_storage_config
from .inputs import ExperimentSectionInputs


@dataclass(frozen=True)
class NormalizedExperimentSections:
    model: ModelConfigSpec | None
    run: RunConfigSpec
    launcher: LauncherConfig
    cluster: ClusterConfig
    env: EnvConfig
    resources: ResourcesConfig
    dispatch: DispatchConfig
    artifacts: ArtifactsConfig
    eval: EvalConfigSpec
    output: OutputConfigSpec
    notify: NotifyConfig
    validation: ValidationConfig
    storage: StorageConfigSpec


def normalize_experiment_sections(
    inputs: ExperimentSectionInputs,
    *,
    batch_shared: BatchSharedSpec | None = None,
) -> NormalizedExperimentSections:
    model = normalize_model_config(
        inputs.model_cfg_raw,
        required=(inputs.run_mode == "model_cli"),
        config_path=inputs.config_path,
    )
    launcher = normalize_launcher(inputs.launcher_cfg_raw)
    cluster = normalize_cluster(inputs.cluster_cfg_raw)
    env = normalize_env(inputs.env_cfg_raw)
    # ``resources`` is always normalized per-spec because only
    # ``max_available_gpus`` is batch-scoped (projected onto
    # ``batch_shared.max_available_gpus``); the remaining fields
    # (max_gpus_per_job, auto_gpu, estimator knobs) are run-scoped and must
    # be free to diverge via sweep axes or replay-level per-run variation.
    resources = normalize_resources(inputs.resources_cfg_raw)

    # ``dispatch`` is entirely batch-scoped today (single field
    # ``group_overflow_policy``).  For authoring runs we inherit it from
    # ``batch_shared`` so sweep-expansion cannot accidentally produce
    # per-run divergence.  Replay reconstructs each run's spec from its
    # stored YAML without a shared anchor, so we normalize from the
    # per-run raw cfg; batch-level resolver enforces consistency later.
    dispatch = (
        batch_shared.dispatch_cfg if batch_shared is not None else normalize_dispatch(inputs.dispatch_cfg_raw)
    )
    artifacts = normalize_artifacts(inputs.artifacts_cfg_raw)
    validation = normalize_validation(inputs.validation_cfg_raw)
    eval_spec = normalize_eval_config(inputs.eval_cfg, config_path=inputs.config_path)
    output = (
        batch_shared.output
        if batch_shared is not None
        else normalize_output_config(inputs.output_cfg_raw, config_path=inputs.config_path)
    )
    notify = batch_shared.notify if batch_shared is not None else normalize_notify(inputs.notify_cfg_raw)
    storage = batch_shared.storage if batch_shared is not None else normalize_storage_config(inputs.storage_cfg_raw)

    if inputs.run_mode == "command":
        validate_external_command_launcher(
            launcher,
            inputs.launcher_cfg_raw,
            config_path=inputs.config_path,
            context_name="command mode",
            runtime_field_name="run.external_runtime",
            launcher_field_name="launcher",
        )

    run_spec = build_run_spec(
        inputs.run_cfg,
        config_path=inputs.config_path,
        run_mode=inputs.run_mode,
    )

    return NormalizedExperimentSections(
        model=model,
        run=run_spec,
        launcher=launcher,
        cluster=cluster,
        env=env,
        resources=resources,
        dispatch=dispatch,
        artifacts=artifacts,
        eval=eval_spec,
        output=output,
        notify=notify,
        validation=validation,
        storage=storage,
    )
