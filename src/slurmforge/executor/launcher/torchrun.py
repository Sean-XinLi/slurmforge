from __future__ import annotations

import shlex

from ...plans import LauncherPlan


def torchrun_python_script_command(
    *,
    python_bin: str,
    script: str,
    script_args: list[str],
    launcher: LauncherPlan,
) -> tuple[list[str] | str, bool]:
    if str(launcher.mode or "single_node") == "multi_node":
        return _torchrun_multi_node_command(
            python_bin=python_bin,
            script=script,
            script_args=script_args,
            launcher=launcher,
        ), True
    torchrun = [
        python_bin,
        "-m",
        "torch.distributed.run",
        "--nnodes",
        str(launcher.nnodes),
        "--nproc-per-node",
        str(launcher.nproc_per_node),
    ]
    if launcher.master_port is not None:
        torchrun.extend(["--master-port", str(launcher.master_port)])
    elif launcher.rendezvous is not None and launcher.rendezvous.port is not None:
        torchrun.extend(["--master-port", str(launcher.rendezvous.port)])
    return [*torchrun, script, *script_args], False


def _torchrun_multi_node_command(
    *,
    python_bin: str,
    script: str,
    script_args: list[str],
    launcher: LauncherPlan,
) -> str:
    rendezvous = launcher.rendezvous
    backend = "c10d" if rendezvous is None else rendezvous.backend
    endpoint = "auto" if rendezvous is None else rendezvous.endpoint
    port = int((None if rendezvous is None else rendezvous.port) or launcher.master_port or 29500)
    nnodes = int(launcher.nnodes or 1)
    nproc_per_node = int(launcher.nproc_per_node or 1)
    if endpoint == "auto":
        endpoint_expr = '"${MASTER_ADDR}:${MASTER_PORT}"'
        prelude = [
            f"MASTER_PORT={shlex.quote(str(port))}",
            'MASTER_ADDR="${MASTER_ADDR:-$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)}"',
            "export MASTER_ADDR MASTER_PORT",
        ]
    else:
        endpoint_expr = shlex.quote(endpoint)
        prelude = [f"MASTER_PORT={shlex.quote(str(port))}", "export MASTER_PORT"]
    inner_parts = [
        'NODE_RANK="${SLURM_PROCID:-0}"',
        "export NODE_RANK",
        "exec",
        shlex.quote(python_bin),
        "-m",
        "torch.distributed.run",
        "--nnodes",
        shlex.quote(str(nnodes)),
        "--nproc-per-node",
        shlex.quote(str(nproc_per_node)),
        "--node-rank",
        '"${NODE_RANK}"',
        "--rdzv-backend",
        shlex.quote(backend),
        "--rdzv-endpoint",
        endpoint_expr,
        shlex.quote(script),
        *(shlex.quote(str(item)) for item in script_args),
    ]
    inner = " ".join(inner_parts)
    srun_args = [str(item) for item in launcher.srun_args]
    srun = [
        "srun",
        "--nodes",
        str(nnodes),
        "--ntasks",
        str(nnodes),
        "--ntasks-per-node",
        "1",
        *srun_args,
        "bash",
        "-lc",
        inner,
    ]
    return "; ".join([*prelude, shlex.join(srun)])
