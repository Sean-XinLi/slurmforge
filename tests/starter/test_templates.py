from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import tempfile
import yaml
from argparse import Namespace
from pathlib import Path


class StarterTests(StageBatchSystemTestCase):
    def _init_args(
        self,
        root: Path,
        *,
        template: str = "train-eval",
        force: bool = False,
    ) -> Namespace:
        return Namespace(
            template=template,
            list_templates=False,
            output=str(root / "experiment.yaml"),
            force=force,
        )

    def _interactive_init_args(self) -> Namespace:
        return Namespace(
            template=None,
            list_templates=False,
            output=None,
            force=False,
        )

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

        template = _bad_template(duplicate_readme)
        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(StarterTemplateError, "duplicate path"),
        ):
            render_starter_files(
                InitRequest(
                    template="bad-template", output=Path(tmp) / "experiment.yaml"
                ),
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
                InitRequest(
                    template="bad-template", output=Path(tmp) / "experiment.yaml"
                ),
                _bad_template(absolute_path),
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
                InitRequest(
                    template="bad-template", output=Path(tmp) / "experiment.yaml"
                ),
                _bad_template(parent_path),
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


def _dry_run_command_for_template(template: str, _root: Path) -> list[str]:
    if template == "train-eval":
        return ["run"]
    if template == "train-only":
        return ["train"]
    return ["eval", "--checkpoint", "checkpoint.pt"]


def _bad_template(file_builder):
    from slurmforge.starter.models import (
        StarterCommandSet,
        StarterReadmePlan,
        StarterTemplate,
    )
    from slurmforge.starter.templates.train_eval import build_config

    return StarterTemplate(
        name="bad-template",
        description="bad",
        config_builder=build_config,
        readme_builder=lambda request: StarterReadmePlan(
            template=request.template,
            commands=StarterCommandSet(
                validate="sforge validate --config experiment.yaml",
                dry_run="sforge run --config experiment.yaml --dry-run=full",
                submit="sforge run --config experiment.yaml",
            ),
            editable_fields=(),
        ),
        file_builders=(file_builder,),
    )
