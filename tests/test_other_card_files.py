"""Non-media card contents surfaced in the --plan-json payload."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy


class ClassifyOtherSourceFileTests(unittest.TestCase):
    def test_known_camera_junk_is_trivial(self):
        self.assertEqual(
            stackcopy.classify_other_source_file("OLYMPUS.CTG", ".ctg", 400),
            "trivial",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file(".DS_Store", "", 6148),
            "trivial",
        )

    def test_tiny_unknown_file_is_trivial(self):
        self.assertEqual(
            stackcopy.classify_other_source_file("MYSTERY.BIN", ".bin", 2048),
            "trivial",
        )

    def test_documents_count_as_data_even_when_small(self):
        self.assertEqual(
            stackcopy.classify_other_source_file("shotlist.txt", ".txt", 120),
            "data",
        )

    def test_large_unknown_file_is_data(self):
        self.assertEqual(
            stackcopy.classify_other_source_file("clip.blob", ".blob", 5_000_000),
            "data",
        )


class PlanJsonOtherFilesTests(unittest.TestCase):
    def _plan(self, builder):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            (card / "DCIM" / "100OLYMP").mkdir(parents=True)
            builder(card / "DCIM" / "100OLYMP")
            env = dict(
                os.environ,
                STACKCOPY_LIGHTROOM_IMPORT_DIR=str(root / "Lightroom"),
                STACKCOPY_STACK_INPUT_DIR=str(root / "StackInput"),
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "stackcopy.py"),
                    "--lightroomimport",
                    str(card),
                    "--plan-json",
                    "--no-stack-detection",
                    "--dry-run",
                ],
                capture_output=True,
                text=True,
                env=env,
            )
        return json.loads(completed.stdout)

    def test_data_and_trivial_files_are_counted_separately(self):
        def build(folder):
            (folder / "P8081885.ORF").write_bytes(b"0" * 9_000_000)
            (folder / "P8081885.JPG").write_bytes(b"0" * 6_000_000)
            (folder / "OLYMPUS.CTG").write_bytes(b"0" * 500)
            (folder / "AUTPRINT.MRK").write_bytes(b"0" * 300)
            (folder / "shotlist.txt").write_bytes(b"0" * 2_000_000)

        payload = self._plan(build)
        self.assertEqual(payload["other_files"], 1)
        self.assertEqual(payload["other_files_trivial"], 2)
        self.assertEqual(payload["other_files_bytes"], 2_000_000)
        self.assertEqual(payload["other_file_kinds"], {".TXT": 1})
        self.assertEqual(payload["other_file_examples"], ["shotlist.txt"])

    def test_media_only_card_reports_zero(self):
        def build(folder):
            (folder / "P8081885.ORF").write_bytes(b"0" * 9_000_000)

        payload = self._plan(build)
        self.assertEqual(payload["other_files"], 0)
        self.assertEqual(payload["other_files_trivial"], 0)
        self.assertEqual(payload["other_file_kinds"], {})


if __name__ == "__main__":
    unittest.main()
