from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import io
import tempfile
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
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

    def test_starter_facade_exports_only_stable_api(self) -> None:
        import slurmforge.starter as starter

        for name in (
            "FilePayload",
            "RenderedFile",
            "StarterWritePlan",
            "StarterTemplate",
            "get_template",
        ):
            self.assertFalse(hasattr(starter, name), name)

    def test_lists_templates_without_writing_files(self) -> None:
        from slurmforge.cli.init import handle_init

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self._init_args(root)
            args.list_templates = True
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_init(args)
            self.assertIn("train-eval:", stdout.getvalue())
            self.assertIn("eval-checkpoint:", stdout.getvalue())
            self.assertFalse((root / "experiment.yaml").exists())

    def test_removed_init_detail_flags_are_rejected(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        stderr = io.StringIO()
        with (
            redirect_stdout(io.StringIO()),
            redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            parser.parse_args(["init", "--project", "demo"])
        self.assertIn("unrecognized arguments: --project", stderr.getvalue())


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

    return StarterTemplate(
        name="bad-template",
        description="bad",
        config_builder=lambda _request: {"project": "demo"},
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
