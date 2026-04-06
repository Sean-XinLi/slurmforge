from __future__ import annotations

RUN_MODES = {"command", "adapter", "model_cli"}
LAUNCH_MODES = {"auto", "ddp", "single"}
EVAL_LAUNCH_MODES = LAUNCH_MODES | {"inherit"}
EVAL_CHECKPOINT_POLICIES = {"latest", "best", "explicit"}
REPLAY_MODEL_CATALOG_KEY = "resolved_model_catalog"
