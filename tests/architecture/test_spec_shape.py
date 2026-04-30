from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class SpecShapeTests(StageBatchSystemTestCase):
    def test_spec_models_resolver_and_snapshot_boundaries_stay_split(self) -> None:
        self.assertFalse(Path("src/slurmforge/spec/models.py").exists())
        self.assertTrue(Path("src/slurmforge/spec/models/experiment.py").exists())
        self.assertFalse(Path("src/slurmforge/resolver/core.py").exists())
        self.assertFalse(Path("src/slurmforge/resolver/sources.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/binding_builders.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/output_refs.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/prior_source.py").exists())

        violations: list[str] = []
        for path in sorted(Path("src/slurmforge").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "_load_snapshot_yaml" in text or "load_snapshot_yaml" in text:
                violations.append(str(path))
            if (
                path != Path("src/slurmforge/spec/snapshot.py")
                and "spec_snapshot.yaml" in text
                and "yaml.safe_load" in text
            ):
                violations.append(f"{path} reads spec snapshots directly")
            if "resolver.core" in text or "resolver.sources" in text:
                violations.append(str(path))
            if (
                "from .core import" in text
                and Path("src/slurmforge/resolver") in path.parents
            ):
                violations.append(str(path))
            if (
                "from .sources import" in text
                and Path("src/slurmforge/resolver") in path.parents
            ):
                violations.append(str(path))
        self.assertEqual(violations, [])

    def test_spec_stage_parsing_is_split_by_section(self) -> None:
        stage_parse_root = Path("src/slurmforge/spec/stage_parse")
        self.assertFalse(Path("src/slurmforge/spec/field_options.py").exists())
        self.assertFalse(Path("src/slurmforge/spec/parse_stages.py").exists())
        self.assertFalse(Path("src/slurmforge/config_schema").exists())
        self.assertTrue(Path("src/slurmforge/config_contract/keys.py").exists())
        self.assertTrue(Path("src/slurmforge/spec/parse_artifact_store.py").exists())
        self.assertTrue(stage_parse_root.is_dir())
        for name in (
            "__init__.py",
            "before.py",
            "entry.py",
            "gpu_sizing.py",
            "inputs.py",
            "launcher.py",
            "stage.py",
        ):
            self.assertTrue((stage_parse_root / name).exists())

    def test_spec_models_do_not_import_parser(self) -> None:
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in Path("src/slurmforge/spec/models").rglob("*.py")
        )
        self.assertNotIn("parse_experiment_spec", text)
        self.assertNotIn("from .parser import", text)

    def test_resolver_explicit_sources_are_split(self) -> None:
        self.assertFalse(Path("src/slurmforge/resolver/explicit.py").exists())
        self.assertTrue(Path("src/slurmforge/resolver/explicit").is_dir())
        for name in ("external_path.py", "stage_batch.py", "run.py"):
            self.assertTrue(Path("src/slurmforge/resolver/explicit", name).exists())
