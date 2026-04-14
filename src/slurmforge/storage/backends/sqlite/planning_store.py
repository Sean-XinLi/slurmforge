from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment

from ....pipeline.records.batch_paths import bind_run_plan_to_batch
from ....pipeline.records.codecs.run_plan import deserialize_run_plan, serialize_run_plan
from ....pipeline.records.codecs.run_snapshot import serialize_run_snapshot
from ....pipeline.planning.contracts import serialize_plan_diagnostic
from ..filesystem.planning_store import FileSystemPlanningStore
from ...descriptor import write_storage_descriptor
from .connection import db_path_for_batch, open_batch_db
from .schema import create_schema, update_meta

if TYPE_CHECKING:
    from ....pipeline.config.api import StorageConfigSpec
    from ....pipeline.materialization.context import MaterializationLayout
    from ....pipeline.records.models.run_plan import RunPlan
    from ....pipeline.records.models.run_snapshot import RunSnapshot
    from ....storage.models import MaterializedBatchBundle


class SqlitePlanningStore(FileSystemPlanningStore):
    """PlanningStore for ``storage.backend.engine = "sqlite"``.

    Carries ``storage_config`` so that all DB path resolution respects the
    user's configured ``sqlite.path``.  Writes the DB from in-memory bundle
    data (not by re-reading staging files).
    """

    def __init__(self, env: Environment, storage_config: StorageConfigSpec | None = None) -> None:
        super().__init__(env)
        self._storage_config = storage_config

    def _resolve_config(self, batch_root: Path) -> StorageConfigSpec:
        """Return the storage config, falling back to auto-detect if not set."""
        if self._storage_config is not None:
            return self._storage_config
        from ...auto_detect import storage_config_for_batch
        return storage_config_for_batch(batch_root)

    # ------------------------------------------------------------------
    # Write path — DB in staging, written from in-memory bundle
    # ------------------------------------------------------------------

    def persist_materialized_batch(
        self,
        bundle: MaterializedBatchBundle,
    ) -> tuple[dict[str, Any], ...]:
        from ....pipeline.materialization.layout import (
            prepare_staging_layout,
            resolve_materialization_layout,
        )
        from ....pipeline.materialization.commit import commit_staging
        from ....pipeline.materialization.layout import map_to_staging

        write_recovery = bundle.storage_config.exports.planning_recovery

        layout = resolve_materialization_layout(bundle.batch_root)
        try:
            prepare_staging_layout(layout)

            # Phase 1: write planning files to staging
            # When planning_recovery=false, skip task_*.json / manifest / snapshots entirely
            array_groups_meta = self._write_planning_to_staging(
                layout, bundle, write_recovery_files=write_recovery,
            )

            # Phase 2: write SQLite DB inside staging — compute path from config
            db_final_path = db_path_for_batch(layout.final_batch_root, bundle.storage_config)
            db_staging_path = map_to_staging(
                db_final_path,
                final_root=layout.final_batch_root,
                staging_root=layout.staging_root,
            )
            self._write_planning_db(db_staging_path, bundle, array_groups_meta, layout)

            # Phase 3: write storage descriptor inside staging
            write_storage_descriptor(layout.staging_root, bundle.storage_config, bundle.batch_root)

            # Phase 4: atomic commit
            commit_staging(layout)
        except Exception:
            shutil.rmtree(layout.staging_root, ignore_errors=True)
            raise
        return tuple(array_groups_meta)

    def _write_planning_db(
        self,
        db_path: Path,
        bundle: MaterializedBatchBundle,
        array_groups_meta: list[dict[str, Any]],
        layout: MaterializationLayout,
    ) -> None:
        """Write all planning data into a SQLite DB directly from the in-memory bundle."""
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with open_batch_db(db_path, config=bundle.storage_config, create=True) as conn:
            create_schema(conn)
            update_meta(
                conn,
                project=bundle.project,
                experiment_name=bundle.experiment_name,
                batch_name=bundle.batch_name,
                batch_root=str(layout.final_batch_root),
                total_runs=bundle.total_runs,
            )
            self._insert_array_groups(conn, array_groups_meta)
            self._insert_runs_from_bundle(conn, bundle, array_groups_meta)
            self._insert_snapshots_from_bundle(conn, bundle)
            self._insert_planning_diagnostics(conn, bundle)

    def _insert_array_groups(self, conn, array_groups_meta) -> None:
        with conn:
            for meta in array_groups_meta:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO array_groups
                        (group_index, group_signature, array_size, sbatch_path, records_dir, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        meta.get("group_index"),
                        meta.get("group_signature", ""),
                        meta.get("array_size", 0),
                        meta.get("sbatch_path", ""),
                        meta.get("records_dir", ""),
                        json.dumps(meta, sort_keys=True),
                    ),
                )

    def _insert_runs_from_bundle(
        self, conn, bundle: MaterializedBatchBundle, array_groups_meta: list[dict[str, Any]],
    ) -> None:
        """Insert runs from in-memory bundle, using array_groups_meta for dispatch fields.

        The bundle's plans don't have finalized dispatch info (array_group,
        array_task_index, sbatch_path, record_path).  We reconstruct these from
        ``array_groups_meta`` before serializing to DB.
        """
        from dataclasses import replace as _replace
        from ....pipeline.records.batch_paths import batch_relative_path

        # Build run_index → (group_index, task_index, group_meta) mapping
        run_index_to_slot: dict[int, tuple[int, int, dict]] = {}
        for meta in array_groups_meta:
            group_idx = meta["group_index"]
            for task_idx, run_index in enumerate(meta["run_indices"]):
                run_index_to_slot[run_index] = (group_idx, task_idx, meta)

        batch_root = bundle.batch_root.resolve()
        write_recovery = bundle.storage_config.exports.planning_recovery

        with conn:
            for planned_run in bundle.planned_runs:
                plan = planned_run.plan
                slot = run_index_to_slot.get(plan.run_index)
                if slot is None:
                    continue
                group_idx, task_idx, group_meta = slot

                sbatch_path = group_meta.get("sbatch_path", "")
                if write_recovery:
                    # Recovery files exist — record_path points to the real file
                    records_dir = batch_root / "records" / f"group_{group_idx:02d}"
                    record_file = records_dir / f"task_{task_idx:06d}.json"
                    record_path = str(record_file)
                    record_path_rel = batch_relative_path(batch_root, record_file)
                else:
                    # Pure DB mode — no record files on disk, don't fabricate paths
                    record_path = ""
                    record_path_rel = None

                dispatch = _replace(
                    plan.dispatch,
                    sbatch_path=sbatch_path,
                    sbatch_path_rel=batch_relative_path(batch_root, sbatch_path) if sbatch_path else None,
                    record_path=record_path,
                    record_path_rel=record_path_rel,
                    array_group=group_idx,
                    array_task_index=task_idx,
                )
                plan_for_db = _replace(plan, dispatch=dispatch)

                conn.execute(
                    """
                    INSERT OR REPLACE INTO runs
                        (run_id, run_index, group_index, task_index, run_dir_rel, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        plan.run_id,
                        plan.run_index,
                        group_idx,
                        task_idx,
                        plan.run_dir_rel or "",
                        json.dumps(serialize_run_plan(plan_for_db), sort_keys=True),
                    ),
                )

    def _insert_snapshots_from_bundle(self, conn, bundle: MaterializedBatchBundle) -> None:
        """Insert snapshots directly from bundle.planned_runs — no file re-reading."""
        with conn:
            for planned_run in bundle.planned_runs:
                snapshot = planned_run.snapshot
                conn.execute(
                    """
                    INSERT OR REPLACE INTO run_snapshots (run_id, payload_json)
                    VALUES (?, ?)
                    """,
                    (
                        snapshot.run_id,
                        json.dumps(serialize_run_snapshot(snapshot), sort_keys=True),
                    ),
                )

    def _insert_planning_diagnostics(self, conn, bundle: MaterializedBatchBundle) -> None:
        with conn:
            for diag in bundle.planning_diagnostics:
                payload = serialize_plan_diagnostic(diag)
                conn.execute(
                    """
                    INSERT INTO planning_diagnostics
                        (run_id, stage, category, code, message, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        None,
                        payload.get("stage", ""),
                        payload.get("category", ""),
                        payload.get("code", ""),
                        payload.get("message", ""),
                        json.dumps(payload, sort_keys=True),
                    ),
                )

    # ------------------------------------------------------------------
    # Read path — DB-first, filesystem fallback ONLY if recovery enabled
    # ------------------------------------------------------------------

    def _can_fallback_to_filesystem(self) -> bool:
        """Return True only if planning_recovery files are expected to exist."""
        if self._storage_config is None:
            return True
        return self._storage_config.exports.planning_recovery

    def load_batch_run_plans(self, batch_root: Path) -> tuple[RunPlan, ...]:
        config = self._resolve_config(batch_root)
        db_path = db_path_for_batch(batch_root, config)
        if db_path.exists():
            with open_batch_db(db_path, config=config) as conn:
                rows = conn.execute(
                    "SELECT payload_json FROM runs ORDER BY run_index"
                ).fetchall()
                if rows:
                    return tuple(
                        bind_run_plan_to_batch(
                            batch_root.resolve(),
                            deserialize_run_plan(json.loads(row["payload_json"])),
                        )
                        for row in rows
                    )
        if self._can_fallback_to_filesystem():
            return super().load_batch_run_plans(batch_root)
        raise FileNotFoundError(
            f"SQLite DB not found or empty at {db_path} and planning_recovery=false "
            f"(no filesystem fallback)"
        )

    def load_run_snapshot(self, batch_root: Path, run_id: str) -> RunSnapshot | None:
        from ....pipeline.records.codecs.run_snapshot import deserialize_run_snapshot

        config = self._resolve_config(batch_root)
        db_path = db_path_for_batch(batch_root, config)
        if db_path.exists():
            with open_batch_db(db_path, config=config) as conn:
                row = conn.execute(
                    "SELECT payload_json FROM run_snapshots WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if row:
                    return deserialize_run_snapshot(json.loads(row["payload_json"]))
        if self._can_fallback_to_filesystem():
            return super().load_run_snapshot(batch_root, run_id)
        return None

    def load_plan_for_array_task(
        self,
        batch_root: Path,
        group_index: int,
        task_index: int,
    ) -> RunPlan | None:
        config = self._resolve_config(batch_root)
        db_path = db_path_for_batch(batch_root, config)
        if db_path.exists():
            with open_batch_db(db_path, config=config) as conn:
                row = conn.execute(
                    "SELECT payload_json FROM runs WHERE group_index = ? AND task_index = ?",
                    (group_index, task_index),
                ).fetchone()
                if row:
                    return bind_run_plan_to_batch(
                        batch_root.resolve(),
                        deserialize_run_plan(json.loads(row["payload_json"])),
                    )
        if self._can_fallback_to_filesystem():
            return super().load_plan_for_array_task(batch_root, group_index, task_index)
        return None
