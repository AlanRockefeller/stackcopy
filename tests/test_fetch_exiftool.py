"""Windows ExifTool payload preparation is complete or fails closed."""

import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packaging"))

import fetch_exiftool  # noqa: E402


class ExifToolPayloadTests(unittest.TestCase):
    def test_reuse_requires_the_executable_and_support_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = Path(tmp) / "exiftool"
            payload.mkdir()
            (payload / "exiftool.exe").write_bytes(b"exe")

            self.assertFalse(fetch_exiftool.payload_is_complete(payload))

            (payload / fetch_exiftool.SUPPORT_DIRECTORY).mkdir()
            self.assertTrue(fetch_exiftool.payload_is_complete(payload))

    def test_main_reextracts_an_executable_only_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = root / "vendor" / "exiftool"
            payload.mkdir(parents=True)
            (payload / "exiftool.exe").write_bytes(b"partial")
            archive = root / "archive.zip"

            with (
                mock.patch.object(fetch_exiftool, "VENDOR_DIR", payload),
                mock.patch.object(fetch_exiftool.sys, "platform", "win32"),
                mock.patch.object(
                    fetch_exiftool.sys,
                    "argv",
                    ["fetch_exiftool.py", "--archive", str(archive)],
                ),
                mock.patch.object(fetch_exiftool, "verify"),
                mock.patch.object(fetch_exiftool, "extract") as extract,
            ):
                self.assertEqual(fetch_exiftool.main(), 0)

            extract.assert_called_once_with(archive, payload)

    def make_archive(self, path: Path, *, complete: bool) -> None:
        root = fetch_exiftool.ARCHIVE_ROOT
        with zipfile.ZipFile(path, "w") as bundle:
            bundle.writestr(f"{root}/{fetch_exiftool.SOURCE_EXECUTABLE}", b"new exe")
            if complete:
                bundle.writestr(
                    f"{root}/{fetch_exiftool.SUPPORT_DIRECTORY}/helper.dll",
                    b"support",
                )

    def test_extract_replaces_the_target_only_after_preparing_a_complete_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "exiftool.zip"
            target = root / "vendor" / "exiftool"
            target.mkdir(parents=True)
            (target / "old.txt").write_text("old", encoding="utf-8")
            self.make_archive(archive, complete=True)

            fetch_exiftool.extract(archive, target)

            self.assertTrue(fetch_exiftool.payload_is_complete(target))
            self.assertEqual((target / "exiftool.exe").read_bytes(), b"new exe")
            self.assertFalse((target / "old.txt").exists())

    def test_invalid_archive_does_not_destroy_an_existing_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "exiftool.zip"
            target = root / "vendor" / "exiftool"
            support = target / fetch_exiftool.SUPPORT_DIRECTORY
            support.mkdir(parents=True)
            (target / "exiftool.exe").write_bytes(b"old exe")
            (support / "helper.dll").write_bytes(b"old support")
            self.make_archive(archive, complete=False)

            with self.assertRaises(RuntimeError):
                fetch_exiftool.extract(archive, target)

            self.assertTrue(fetch_exiftool.payload_is_complete(target))
            self.assertEqual((target / "exiftool.exe").read_bytes(), b"old exe")
            self.assertEqual((support / "helper.dll").read_bytes(), b"old support")


if __name__ == "__main__":
    unittest.main()
