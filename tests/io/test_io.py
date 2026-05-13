from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
import tempfile
from pathlib import Path


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

    def test_read_json_object_rejects_non_object_documents(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.io import read_json_object, read_json_value

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "payload.json"
            path.write_text('{"ok": true}', encoding="utf-8")

            self.assertEqual(read_json_object(path), {"ok": True})

            for raw in ("[]", "null", '"value"', "1"):
                with self.subTest(raw=raw):
                    path.write_text(raw, encoding="utf-8")
                    read_json_value(path)
                    with self.assertRaises(RecordContractError):
                        read_json_object(path)

    def test_write_json_object_rejects_non_object_documents(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.io import read_json_value, write_json_object, write_json_value

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "payload.json"

            write_json_object(path, {"ok": True})
            self.assertEqual(read_json_value(path), {"ok": True})

            for value in ([], None, "value", 1):
                with self.subTest(value=value):
                    with self.assertRaises(RecordContractError):
                        write_json_object(path, value)

            write_json_value(path, ["freeform"])
            self.assertEqual(read_json_value(path), ["freeform"])

    def test_require_schema_rejects_non_integer_versions(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.io import require_schema

        for value in ("1", 1.0, True):
            with self.subTest(value=value):
                with self.assertRaises(RecordContractError):
                    require_schema({"schema_version": value}, name="record", version=1)

    def test_required_array_rejects_in_memory_tuple(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.record_fields import required_array

        with self.assertRaises(RecordContractError):
            required_array({"items": ("a",)}, "items", label="record")
