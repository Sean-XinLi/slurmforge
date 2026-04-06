from __future__ import annotations

import math
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

import yaml

from .catalog import ModelSpec
from ..errors import ConfigContractError
from ..pipeline.config.normalize import ensure_resources_config
from ..pipeline.config.runtime import ResourcesConfig


_YAML_ERROR = getattr(yaml, "YAMLError", None)
if isinstance(_YAML_ERROR, type) and issubclass(_YAML_ERROR, BaseException):
    _MODEL_DEFAULT_LOAD_ERRORS: tuple[type[BaseException], ...] = (
        OSError,
        UnicodeError,
        ValueError,
        TypeError,
        _YAML_ERROR,
    )
else:
    _MODEL_DEFAULT_LOAD_ERRORS = (OSError, UnicodeError, ValueError, TypeError)


def _deep_get(d: dict[str, Any], path: str) -> Any:
    cur: Any = d
    for token in path.split("."):
        if not isinstance(cur, dict) or token not in cur:
            return None
        cur = cur[token]
    return cur


def _pick_number(sources: list[dict[str, Any]], keys: list[str], default: float) -> float:
    for key in keys:
        for source in sources:
            if not isinstance(source, dict):
                continue
            if "." in key:
                value = _deep_get(source, key)
            else:
                value = source.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                return float(value)
    return float(default)


def _pick_text(sources: list[dict[str, Any]], keys: list[str], default: str) -> str:
    for key in keys:
        for source in sources:
            if not isinstance(source, dict):
                continue
            if "." in key:
                value = _deep_get(source, key)
            else:
                value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return default


def _pick_optional_bool(sources: list[dict[str, Any]], keys: list[str]) -> bool | None:
    for key in keys:
        for source in sources:
            if not isinstance(source, dict):
                continue
            value = _deep_get(source, key) if "." in key else source.get(key)
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                text = value.strip().lower()
                if text in {"true", "1", "yes", "y", "on"}:
                    return True
                if text in {"false", "0", "no", "n", "off"}:
                    return False
    return None


def load_model_defaults(spec: ModelSpec) -> dict[str, Any]:
    if spec.yaml_path is None:
        return {}
    path = Path(spec.yaml_path)
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except _MODEL_DEFAULT_LOAD_ERRORS as exc:
        warnings.warn(
            f"failed to load model defaults from {path}: {exc}",
            stacklevel=2,
        )
        return {}
    if not isinstance(payload, dict):
        return {}

    model_block = payload.get("model", {}) or {}
    if not isinstance(model_block, dict):
        return {}

    defaults = model_block.get(spec.name)
    if isinstance(defaults, dict):
        return defaults

    declared_type = model_block.get("type")
    if isinstance(declared_type, str):
        typed_defaults = model_block.get(declared_type)
        if isinstance(typed_defaults, dict):
            return typed_defaults
    return {}


@dataclass(frozen=True)
class GpuEstimate:
    min_total_gpus: int
    recommended_total_gpus: int
    max_useful_total_gpus: int
    estimated_vram_gb: float
    reason: str

    def __post_init__(self) -> None:
        min_total_gpus = max(1, int(self.min_total_gpus))
        recommended_total_gpus = max(min_total_gpus, int(self.recommended_total_gpus))
        max_useful_total_gpus = max(recommended_total_gpus, int(self.max_useful_total_gpus))
        estimated_vram_gb = float(self.estimated_vram_gb)
        reason = str(self.reason or "").strip()
        if not reason:
            raise ConfigContractError("GpuEstimate.reason must be non-empty")
        object.__setattr__(self, "min_total_gpus", min_total_gpus)
        object.__setattr__(self, "recommended_total_gpus", recommended_total_gpus)
        object.__setattr__(self, "max_useful_total_gpus", max_useful_total_gpus)
        object.__setattr__(self, "estimated_vram_gb", estimated_vram_gb)
        object.__setattr__(self, "reason", reason)


class GpuEstimator(Protocol):
    def estimate(
        self,
        model_spec: ModelSpec,
        run_args: dict[str, Any],
        model_overrides: dict[str, Any],
        model_defaults: dict[str, Any],
        resources_cfg: ResourcesConfig | dict[str, Any],
    ) -> GpuEstimate:
        ...


class HeuristicGpuEstimator:
    PROFILE_MULTIPLIERS = {
        "default": 1.0,
        "transformer": 1.0,
        "conv": 1.05,
        "recurrent": 1.1,
        "hybrid": 1.15,
        "long_context": 1.2,
        "memory_efficient": 0.9,
        "heavyweight": 1.3,
    }
    # Conservative proxy for activations retained per transformer block during
    # training. This rolls residual stream, attention projections/scores/context,
    # FFN intermediates, and a small amount of normalization/bias state into one
    # coarse multiplier instead of pretending the estimate is architecture exact.
    ACTIVATION_TENSORS_PER_LAYER = 12.0
    # Gradient checkpointing reduces saved activations materially, but the exact
    # gain depends on the model and framework implementation. Use a conservative
    # 0.6x scale rather than assuming maximal savings.
    GRADIENT_CHECKPOINTING_ACTIVATION_SCALE = 0.6

    def estimate(
        self,
        model_spec: ModelSpec,
        run_args: dict[str, Any],
        model_overrides: dict[str, Any],
        model_defaults: dict[str, Any],
        resources_cfg: ResourcesConfig | dict[str, Any],
    ) -> GpuEstimate:
        resources = ensure_resources_config(resources_cfg)
        sources = [run_args, model_overrides, model_defaults]

        seq_len = _pick_number(sources, ["max_length", "seq_len", "sequence_length"], default=512)
        batch_size = _pick_number(
            sources,
            ["batch_size", "per_device_train_batch_size", "train_batch_size"],
            default=32,
        )
        layers = _pick_number(
            sources,
            ["num_hidden_layers", "n_layer", "num_layers"],
            default=12,
        )
        hidden = _pick_number(
            sources,
            ["hidden_size", "d_model", "embedding_size", "layer.d_model"],
            default=512,
        )
        inner = _pick_number(sources, ["d_inner"], default=hidden * 4)
        vocab_size = _pick_number(sources, ["vocab_size", "n_vocab", "vocab"], default=0)

        amp_dtype = _pick_text(sources, ["amp_dtype", "dtype", "precision"], default="fp32").lower()
        use_amp = _pick_optional_bool(sources, ["use_amp"])
        if use_amp is False:
            amp_dtype = "fp32"
        gradient_checkpointing = _pick_optional_bool(
            sources,
            [
                "use_gradient_checkpointing",
                "gradient_checkpointing",
                "gradient_checkpointing_enable",
                "checkpoint_activations",
                "activation_checkpointing",
            ],
        )

        dtype_bytes = 2.0 if amp_dtype in {"fp16", "bf16", "float16", "bfloat16"} else 4.0

        # Per-layer parameter proxy: attention ~4*H^2, FFN ~2*H*inner, LayerNorm/bias ~4*H.
        block_params = layers * (4.0 * hidden * hidden + 2.0 * hidden * inner + 4.0 * hidden)
        embedding_params = max(vocab_size, 0.0) * hidden
        approx_params_m = max(1.0, (block_params + embedding_params) / 1_000_000.0)
        # Mixed precision keeps optimizer state in fp32; account for it explicitly.
        grad_bytes = 4.0
        master_param_bytes = 4.0 if dtype_bytes < 4.0 else 0.0
        optimizer_state_bytes = 8.0  # Adam m/v in fp32
        state_bytes_per_param = dtype_bytes + grad_bytes + master_param_bytes + optimizer_state_bytes
        model_state_gb = approx_params_m * 1_000_000.0 * state_bytes_per_param / 1_000_000_000.0
        # Activation memory proxy scales with batch, sequence, hidden, and layers.
        activation_scale = (
            self.GRADIENT_CHECKPOINTING_ACTIVATION_SCALE if gradient_checkpointing is True else 1.0
        )
        activation_gb = (
            batch_size
            * seq_len
            * hidden
            * max(layers, 1.0)
            * dtype_bytes
            * self.ACTIVATION_TENSORS_PER_LAYER
            * activation_scale
            / 1_000_000_000.0
        )

        profile_key = model_spec.estimator_profile or "default"
        profile_multiplier = _pick_number(
            sources,
            ["estimator_multiplier", "gpu_profile_multiplier"],
            default=self.PROFILE_MULTIPLIERS.get(profile_key, self.PROFILE_MULTIPLIERS["default"]),
        )
        safety_factor = float(resources.safety_factor)
        target_mem_per_gpu = float(resources.target_mem_per_gpu_gb)
        min_gpus = int(resources.min_gpus_per_job)
        max_gpus = int(resources.max_gpus_per_job)

        total_gb = (model_state_gb + activation_gb) * profile_multiplier * safety_factor
        memory_fit_min_gpus = int(math.ceil(max(1.0, total_gb) / max(1.0, target_mem_per_gpu)))
        if seq_len >= 4096:
            preferred_samples_per_gpu = 4
        elif seq_len >= 2048:
            preferred_samples_per_gpu = 8
        elif seq_len >= 1024:
            preferred_samples_per_gpu = 16
        else:
            preferred_samples_per_gpu = 32
        preferred_samples_per_gpu = max(1, min(32, preferred_samples_per_gpu))
        minimum_efficient_samples_per_gpu = max(1, preferred_samples_per_gpu // 4)
        throughput_recommended_gpus = int(math.ceil(max(1.0, batch_size) / preferred_samples_per_gpu))
        throughput_max_useful_gpus = int(math.ceil(max(1.0, batch_size) / minimum_efficient_samples_per_gpu))
        feasible_min = max(min_gpus, min(max_gpus, memory_fit_min_gpus))
        recommended = max(feasible_min, throughput_recommended_gpus)
        recommended = max(min_gpus, min(max_gpus, recommended))
        max_useful = max(recommended, throughput_max_useful_gpus)
        max_useful = max(min_gpus, min(max_gpus, max_useful))

        reason = (
            f"profile={profile_key}, seq={int(seq_len)}, batch={int(batch_size)}, "
            f"layers={int(layers)}, hidden={int(hidden)}, inner={int(inner)}, vocab={int(vocab_size)}, dtype={amp_dtype}, "
            f"grad_ckpt={'on' if gradient_checkpointing is True else 'off'}, "
            f"approx_params={approx_params_m:.1f}M, model_state={model_state_gb:.2f}GB, "
            f"activations={activation_gb:.2f}GB, "
            f"estimated_total_vram={total_gb:.2f}GB, "
            f"memory_fit_min_gpus={memory_fit_min_gpus}, preferred_samples_per_gpu={preferred_samples_per_gpu}, "
            f"throughput_recommended_gpus={throughput_recommended_gpus}, "
            f"throughput_max_useful_gpus={throughput_max_useful_gpus}"
        )
        return GpuEstimate(
            min_total_gpus=feasible_min,
            recommended_total_gpus=recommended,
            max_useful_total_gpus=max_useful,
            estimated_vram_gb=round(total_gb, 2),
            reason=reason,
        )

EstimatorFactory = Callable[[], GpuEstimator]

_ESTIMATOR_FACTORIES: dict[str, EstimatorFactory] = {}


def register_estimator(name: str, factory: EstimatorFactory) -> None:
    normalized = (name or "").strip().lower()
    if not normalized:
        raise ConfigContractError("gpu estimator name must be non-empty")
    _ESTIMATOR_FACTORIES[normalized] = factory


def registered_estimators() -> tuple[str, ...]:
    return tuple(sorted(_ESTIMATOR_FACTORIES))


def build_estimator(name: str) -> GpuEstimator:
    normalized = (name or "heuristic").strip().lower()
    factory = _ESTIMATOR_FACTORIES.get(normalized)
    if factory is None and normalized == "default":
        factory = _ESTIMATOR_FACTORIES.get("heuristic")
    if factory is None:
        supported = ", ".join(registered_estimators()) or "none"
        raise ConfigContractError(f"Unsupported gpu estimator `{normalized}`. Supported: {supported}")
    return factory()


register_estimator("heuristic", HeuristicGpuEstimator)
