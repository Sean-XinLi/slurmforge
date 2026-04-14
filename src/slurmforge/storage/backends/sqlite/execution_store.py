from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from ...execution.journal import (
    read_attempt_result,
    read_checkpoint_state,
    read_execution_status,
    read_latest_result_dir,
    read_artifact_manifest,
    read_train_outputs_manifest,
)
from ...execution.paths import (
    artifact_manifest_path,
    attempt_result_path,
    checkpoint_state_path,
    execution_status_path,
    train_outputs_manifest_path,
)
from ..filesystem.execution_store import FileSystemExecutionStore
from .connection import db_path_for_batch, open_batch_db
from .schema import create_schema

if TYPE_CHECKING:
    from ....pipeline.config.api import StorageConfigSpec
    from ....storage.models import LatestAttemptRecord

from ....pipeline.status.codecs import (
    deserialize_execution_status as _deserialize_execution_status,
    serialize_attempt_result,
    serialize_execution_status,
)


class SqliteExecutionStore(FileSystemExecutionStore):
    """ExecutionStore for ``storage.backend.engine = "sqlite"``."""

    def __init__(self, storage_config: StorageConfigSpec) -> None:
        self._storage_config = storage_config

    # ------------------------------------------------------------------
    # Reconcile — scans ALL job-* dirs, marks latest from pointer
    # ------------------------------------------------------------------

    def reconcile_batch(self, batch_root: Path) -> None:
        db_path = db_path_for_batch(batch_root, self._storage_config)
        if not db_path.exists():
            return

        resolved_root = batch_root.resolve()

        with open_batch_db(db_path, config=self._storage_config) as conn:
            create_schema(conn)

            rows = conn.execute("SELECT run_id, run_dir_rel FROM runs").fetchall()
            if not rows:
                return

            for row in rows:
                run_id = row["run_id"]
                run_dir_rel = row["run_dir_rel"]
                if not run_dir_rel:
                    continue
                run_dir = (resolved_root / run_dir_rel).resolve()
                if not run_dir.exists():
                    continue

                # Determine which result_dir the latest pointer says is current
                latest_result_dir = read_latest_result_dir(run_dir)
                latest_result_dir_rel = None
                if latest_result_dir is not None:
                    try:
                        latest_result_dir_rel = latest_result_dir.resolve().relative_to(resolved_root).as_posix()
                    except ValueError:
                        pass

                # Scan ALL job-* result dirs for this run
                for result_dir in sorted(run_dir.glob("job-*")):
                    if not result_dir.is_dir():
                        continue
                    self._reconcile_result_dir(conn, resolved_root, result_dir, run_id)

                # Now set is_latest correctly: clear all, then set the one
                with conn:
                    conn.execute(
                        "UPDATE attempts SET is_latest = 0 WHERE run_id = ?",
                        (run_id,),
                    )
                    if latest_result_dir_rel:
                        conn.execute(
                            "UPDATE attempts SET is_latest = 1 WHERE run_id = ? AND result_dir_rel = ?",
                            (run_id, latest_result_dir_rel),
                        )

    def _reconcile_result_dir(
        self,
        conn,
        resolved_root: Path,
        result_dir: Path,
        run_id: str,
    ) -> None:
        """Ingest a single result_dir into the attempts table."""
        result_dir_resolved = result_dir.resolve()
        try:
            result_dir_rel = result_dir_resolved.relative_to(resolved_root).as_posix()
        except ValueError:
            result_dir_rel = str(result_dir_resolved)

        files_to_check = [
            execution_status_path(result_dir),
            attempt_result_path(result_dir),
            checkpoint_state_path(result_dir),
            train_outputs_manifest_path(result_dir),
            artifact_manifest_path(result_dir),
        ]

        any_changed = any(self._file_changed(conn, f) for f in files_to_check)
        if not any_changed:
            return

        status = read_execution_status(result_dir)
        attempt = read_attempt_result(result_dir)
        ckpt = read_checkpoint_state(result_dir)
        tom = read_train_outputs_manifest(result_dir)
        artifact = read_artifact_manifest(result_dir)

        job_key = ""
        if status is not None:
            job_key = status.job_key
        elif attempt is not None:
            job_key = attempt.job_key

        status_json_str = json.dumps(serialize_execution_status(status), sort_keys=True) if status else None
        attempt_json_str = json.dumps(serialize_attempt_result(attempt), sort_keys=True) if attempt else None

        from ....pipeline.checkpoints.codec import serialize_checkpoint_state
        ckpt_json = json.dumps(serialize_checkpoint_state(ckpt), sort_keys=True) if ckpt else None

        from ....pipeline.train_outputs.codec import serialize_train_outputs_manifest
        tom_json = json.dumps(serialize_train_outputs_manifest(tom), sort_keys=True) if tom else None

        artifact_json = json.dumps(artifact, sort_keys=True) if artifact else None

        # is_latest=0 here; the caller sets the correct latest after all dirs are scanned
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO attempts (
                    run_id, result_dir_rel, job_key, is_latest,
                    state, failure_class, failed_stage, reason, shell_exit_code,
                    slurm_job_id, slurm_array_job_id, slurm_array_task_id,
                    started_at, finished_at,
                    latest_checkpoint, selection_reason,
                    train_outputs_status, primary_checkpoint,
                    artifact_status,
                    status_json, attempt_result_json,
                    checkpoint_json, train_outputs_json, artifact_json
                ) VALUES (?, ?, ?, 0,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?,
                    ?, ?,
                    ?, ?, ?)
                """,
                (
                    run_id, result_dir_rel, job_key,
                    status.state if status else None,
                    status.failure_class if status else None,
                    status.failed_stage if status else None,
                    status.reason if status else None,
                    status.shell_exit_code if status else None,
                    status.slurm_job_id if status else None,
                    status.slurm_array_job_id if status else None,
                    status.slurm_array_task_id if status else None,
                    status.started_at if status else None,
                    status.finished_at if status else None,
                    ckpt.latest_checkpoint_rel if ckpt else None,
                    ckpt.selection_reason if ckpt else None,
                    tom.status if tom else None,
                    tom.primary_checkpoint if tom else None,
                    artifact.get("status") if artifact else None,
                    status_json_str, attempt_json_str,
                    ckpt_json, tom_json, artifact_json,
                ),
            )

        for f in files_to_check:
            self._mark_ingested(conn, f)

    def _file_changed(self, conn, path: Path) -> bool:
        if not path.exists():
            return False
        try:
            stat = path.stat()
        except OSError:
            return False
        row = conn.execute(
            "SELECT file_mtime_ns, file_size FROM reconcile_state WHERE file_path = ?",
            (str(path.resolve()),),
        ).fetchone()
        if row is None:
            return True
        return row["file_mtime_ns"] != stat.st_mtime_ns or row["file_size"] != stat.st_size

    def _mark_ingested(self, conn, path: Path) -> None:
        if not path.exists():
            return
        try:
            stat = path.stat()
        except OSError:
            return
        conn.execute(
            "INSERT OR REPLACE INTO reconcile_state (file_path, file_mtime_ns, file_size) VALUES (?, ?, ?)",
            (str(path.resolve()), stat.st_mtime_ns, stat.st_size),
        )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def load_latest_attempts(
        self,
        batch_root: Path,
        run_id_to_run_dir: dict[str, Path],
    ) -> dict[str, LatestAttemptRecord]:
        from ...models import LatestAttemptRecord

        db_path = db_path_for_batch(batch_root, self._storage_config)
        if not db_path.exists():
            return super().load_latest_attempts(batch_root, run_id_to_run_dir)

        self.reconcile_batch(batch_root)

        with open_batch_db(db_path, config=self._storage_config) as conn:
            rows = conn.execute(
                "SELECT run_id, result_dir_rel, status_json FROM attempts WHERE is_latest = 1"
            ).fetchall()

        resolved_root = batch_root.resolve()
        results: dict[str, LatestAttemptRecord] = {}
        for row in rows:
            status = None
            if row["status_json"]:
                # Let deserialization errors propagate — corrupt status_json
                # in the DB is a real error, not "no status".
                result_dir_abs = resolved_root / row["result_dir_rel"]
                status = _deserialize_execution_status(
                    json.loads(row["status_json"]), result_dir=result_dir_abs,
                )
            results[row["run_id"]] = LatestAttemptRecord(
                run_id=row["run_id"],
                latest_result_dir=str(resolved_root / row["result_dir_rel"]) if row["result_dir_rel"] else "",
                latest_status=status,
            )
        return results
