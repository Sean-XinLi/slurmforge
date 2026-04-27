from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.std import Path, tempfile


class IoPrimitiveTests(StageBatchSystemTestCase):
    def test_stable_json_and_digest_are_order_insensitive(self) -> None:
        from slurmforge.io import content_digest, stable_json

        left = {"b": [2, 1], "a": {"z": 3}}
        right = {"a": {"z": 3}, "b": [2, 1]}

        self.assertEqual(stable_json(left), stable_json(right))
        self.assertEqual(content_digest(left), content_digest(right))
        self.assertEqual(len(content_digest(left, prefix=12)), 12)

    def test_file_digest_hashes_file_content(self) -> None:
        from slurmforge.io import file_digest

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "payload.txt"
            path.write_text("abc", encoding="utf-8")

            self.assertEqual(
                file_digest(path),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )
