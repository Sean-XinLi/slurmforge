from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase


class SpecFacadeTests(StageBatchSystemTestCase):
    def test_shared_types_have_single_public_import_path(self) -> None:
        import slurmforge.plans as plans
        import slurmforge.spec as spec

        for module in (plans, spec):
            exported = set(getattr(module, "__all__", ()))
            self.assertNotIn("InputSource", exported)
            self.assertNotIn("InputInjection", exported)
            self.assertNotIn("to_jsonable", exported)
            self.assertNotIn("stable_json", exported)
