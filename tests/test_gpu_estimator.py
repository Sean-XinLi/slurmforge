from __future__ import annotations

import unittest
import warnings
from pathlib import Path
from unittest.mock import patch


from slurmforge.model_support.gpu_estimator import GpuEstimate, HeuristicGpuEstimator, load_model_defaults
from slurmforge.model_support.gpu_estimator import build_estimator, register_estimator
from slurmforge.model_support.catalog import ModelSpec


class GpuEstimatorTests(unittest.TestCase):
    def test_gpu_estimator_parses_string_bool_and_vocab(self) -> None:
        estimator = HeuristicGpuEstimator()
        estimate = estimator.estimate(
            model_spec=ModelSpec("demo", Path("train.py"), None, True, False, "default"),
            run_args={
                "batch_size": 8,
                "max_length": 128,
                "hidden_size": 256,
                "num_layers": 4,
                "d_inner": 1024,
                "vocab_size": 32000,
                "use_amp": "false",
            },
            model_overrides={},
            model_defaults={},
            resources_cfg={"target_mem_per_gpu_gb": 40, "min_gpus_per_job": 1, "max_gpus_per_job": 8},
        )
        self.assertIsInstance(estimate, GpuEstimate)
        self.assertIn("dtype=fp32", estimate.reason)
        self.assertIn("vocab=32000", estimate.reason)

    def test_load_model_defaults_warns_on_parse_failure(self) -> None:
        spec = ModelSpec("demo", Path("train.py"), Path("defaults.yaml"), True, False, "default")
        with patch("slurmforge.model_support.gpu_estimator.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text", return_value="broken: ["):
                with patch("slurmforge.model_support.gpu_estimator.yaml.safe_load", side_effect=ValueError("bad yaml")):
                    with warnings.catch_warnings(record=True) as records:
                        warnings.simplefilter("always")
                        defaults = load_model_defaults(spec)
        self.assertEqual(defaults, {})
        self.assertTrue(records)
        self.assertIn("failed to load model defaults", str(records[0].message))

    def test_gpu_estimator_scales_activation_proxy_when_gradient_checkpointing_enabled(self) -> None:
        estimator = HeuristicGpuEstimator()
        base_kwargs = dict(
            model_spec=ModelSpec("demo", Path("train.py"), None, True, False, "default"),
            model_overrides={},
            model_defaults={},
            resources_cfg={"target_mem_per_gpu_gb": 40, "min_gpus_per_job": 1, "max_gpus_per_job": 8},
        )
        without_ckpt = estimator.estimate(
            run_args={
                "batch_size": 16,
                "max_length": 512,
                "hidden_size": 768,
                "num_layers": 12,
            },
            **base_kwargs,
        )
        with_ckpt = estimator.estimate(
            run_args={
                "batch_size": 16,
                "max_length": 512,
                "hidden_size": 768,
                "num_layers": 12,
                "use_gradient_checkpointing": True,
            },
            **base_kwargs,
        )
        self.assertLess(with_ckpt.estimated_vram_gb, without_ckpt.estimated_vram_gb)
        self.assertIn("grad_ckpt=on", with_ckpt.reason)
        self.assertIn("grad_ckpt=off", without_ckpt.reason)

    def test_gpu_estimator_uses_generic_profile_names(self) -> None:
        estimator = HeuristicGpuEstimator()
        estimate = estimator.estimate(
            model_spec=ModelSpec("demo", Path("train.py"), None, True, False, "long_context"),
            run_args={"batch_size": 8, "max_length": 1024, "hidden_size": 256, "num_layers": 4},
            model_overrides={},
            model_defaults={},
            resources_cfg={"target_mem_per_gpu_gb": 40, "min_gpus_per_job": 1, "max_gpus_per_job": 8},
        )
        self.assertIn("profile=long_context", estimate.reason)

    def test_gpu_estimator_returns_layered_gpu_envelope(self) -> None:
        estimator = HeuristicGpuEstimator()
        estimate = estimator.estimate(
            model_spec=ModelSpec("demo", Path("train.py"), None, True, False, "default"),
            run_args={"batch_size": 256, "max_length": 512, "hidden_size": 128, "num_layers": 2},
            model_overrides={},
            model_defaults={},
            resources_cfg={
                "target_mem_per_gpu_gb": 40,
                "min_gpus_per_job": 1,
                "max_gpus_per_job": 32,
                "max_available_gpus": 32,
            },
        )
        self.assertLessEqual(estimate.min_total_gpus, estimate.recommended_total_gpus)
        self.assertLess(estimate.recommended_total_gpus, estimate.max_useful_total_gpus)
        self.assertIn("throughput_recommended_gpus=", estimate.reason)
        self.assertIn("throughput_max_useful_gpus=", estimate.reason)

    def test_build_estimator_supports_registry(self) -> None:
        class TinyEstimator:
            def estimate(self, **_kwargs):
                return GpuEstimate(
                    min_total_gpus=1,
                    recommended_total_gpus=1,
                    max_useful_total_gpus=1,
                    estimated_vram_gb=1.0,
                    reason="tiny",
                )

        register_estimator("tiny", TinyEstimator)
        estimator = build_estimator("tiny")
        estimate = estimator.estimate()
        self.assertEqual(estimate.reason, "tiny")
