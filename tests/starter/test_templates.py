from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.starter.helpers import bad_template
import tempfile
import yaml
from pathlib import Path


class StarterTests(StageBatchSystemTestCase):
    def test_render_rejects_duplicate_template_paths(self) -> None:
        from slurmforge.starter.errors import StarterTemplateError
        from slurmforge.starter.models import FilePayload, InitRequest
        from slurmforge.starter.render import render_starter_files

        def duplicate_readme(_request: InitRequest) -> FilePayload:
            return FilePayload(
                relative_path=Path("README.sforge.md"),
                content="duplicate\n",
                role="guide",
            )

        template = bad_template(duplicate_readme)
        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(StarterTemplateError, "duplicate path"),
        ):
            render_starter_files(
                InitRequest(template="bad-template", output_dir=Path(tmp)),
                template,
            )

    def test_render_rejects_absolute_template_paths(self) -> None:
        from slurmforge.starter.errors import StarterTemplateError
        from slurmforge.starter.models import FilePayload, InitRequest
        from slurmforge.starter.render import render_starter_files

        def absolute_path(_request: InitRequest) -> FilePayload:
            return FilePayload(
                relative_path=Path("/tmp/outside.py"), content="bad\n", role="script"
            )

        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(StarterTemplateError, "must be relative"),
        ):
            render_starter_files(
                InitRequest(template="bad-template", output_dir=Path(tmp)),
                bad_template(absolute_path),
            )

    def test_render_rejects_parent_template_paths(self) -> None:
        from slurmforge.starter.errors import StarterTemplateError
        from slurmforge.starter.models import FilePayload, InitRequest
        from slurmforge.starter.render import render_starter_files

        def parent_path(_request: InitRequest) -> FilePayload:
            return FilePayload(
                relative_path=Path("../outside.py"), content="bad\n", role="script"
            )

        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(StarterTemplateError, "output root"),
        ):
            render_starter_files(
                InitRequest(template="bad-template", output_dir=Path(tmp)),
                bad_template(parent_path),
            )

    def test_demo_fixture_replaces_default_sections(self) -> None:
        from tests.helpers.configs import write_demo_project

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = write_demo_project(
                Path(tmp),
                extra={
                    "runs": {"type": "cases", "cases": [{"train.entry.args.lr": 0.01}]}
                },
            )

            payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["runs"]["type"], "cases")
            self.assertNotIn("axes", payload["runs"])

    def test_demo_fixture_replaces_dotted_sections(self) -> None:
        from tests.helpers.configs import write_demo_project

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = write_demo_project(
                Path(tmp),
                extra={
                    "stages": {
                        "eval": {
                            "outputs": {
                                "custom_metric": {
                                    "kind": "metric",
                                    "file": "eval/metrics.json",
                                    "json_path": "$.custom",
                                }
                            }
                        }
                    }
                },
                replace_sections=("runs", "stages.eval.outputs"),
            )

            outputs = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["stages"][
                "eval"
            ]["outputs"]
            self.assertEqual(tuple(outputs), ("custom_metric",))

    def test_demo_fixture_deep_merges_non_replace_sections(self) -> None:
        from tests.helpers.configs import write_demo_project

        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = write_demo_project(
                Path(tmp),
                extra={"stages": {"train": {"resources": {"mem": "64G"}}}},
            )

            resources = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["stages"][
                "train"
            ]["resources"]
            self.assertEqual(resources["mem"], "64G")
            self.assertEqual(resources["constraint"], "base")
