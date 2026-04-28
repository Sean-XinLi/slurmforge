from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    load_experiment_spec,
    write_demo_project,
)
import tempfile
from pathlib import Path


class SpecSnapshotTests(StageBatchSystemTestCase):
    def test_load_spec_snapshot_validates_shape(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.spec.snapshot import load_spec_snapshot

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(
                ConfigContractError, "spec_snapshot.yaml not found"
            ):
                load_spec_snapshot(root)

            (root / "spec_snapshot.yaml").write_text("[]\n", encoding="utf-8")
            with self.assertRaisesRegex(ConfigContractError, "must contain a mapping"):
                load_spec_snapshot(root)

    def test_load_experiment_spec_from_snapshot_uses_single_loader(self) -> None:
        from slurmforge.spec.snapshot import load_experiment_spec_from_snapshot

        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            cfg_path = write_demo_project(project_root)
            expected = load_experiment_spec(cfg_path)
            snapshot_root = project_root / "snapshot-root"
            snapshot_root.mkdir()
            (snapshot_root / "spec_snapshot.yaml").write_text(
                cfg_path.read_text(encoding="utf-8"), encoding="utf-8"
            )

            loaded = load_experiment_spec_from_snapshot(
                snapshot_root, project_root=project_root
            )

            self.assertEqual(loaded.project, expected.project)
            self.assertEqual(loaded.stage_order(), expected.stage_order())
