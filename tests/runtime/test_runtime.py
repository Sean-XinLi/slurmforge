from __future__ import annotations

from tests.support import *  # noqa: F401,F403


class RuntimeProbeTests(StageBatchSystemTestCase):
    def test_missing_python_runtime_fails_cleanly(self) -> None:
        from slurmforge.runtime import probe_python_runtime

        record = probe_python_runtime("__definitely_missing_python__", min_version="3.10")

        self.assertEqual(record.state, "failed")
        self.assertIn("was not found", record.reason)

