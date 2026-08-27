"""Non-media card contents surfaced in the --plan-json payload."""

import contextlib
import io
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

    def test_hidden_data_files_are_preserved_regardless_of_dot_prefix(self):
        # A recognized data extension must win even when the name is a dotfile.
        self.assertEqual(
            stackcopy.classify_other_source_file(".shotlist.txt", ".txt", 120),
            "data",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file(".settings.xmp", ".xmp", 200),
            "data",
        )

    def test_camera_junk_extension_only_trivial_while_small(self):
        limit = stackcopy.TRIVIAL_OTHER_FILE_BYTES
        self.assertEqual(
            stackcopy.classify_other_source_file(".mystery.bin", ".bin", 2048),
            "trivial",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file("MYSTERY.BIN", ".bin", limit + 1),
            "data",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file("clip.xml", ".xml", limit + 1),
            "data",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file("camera.log", ".log", limit + 1),
            "data",
        )

    def test_known_housekeeping_names_stay_trivial_even_when_large(self):
        limit = stackcopy.TRIVIAL_OTHER_FILE_BYTES
        self.assertEqual(
            stackcopy.classify_other_source_file(".DS_Store", "", limit + 1),
            "trivial",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file("desktop.ini", ".ini", limit + 1),
            "trivial",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file("Thumbs.db", ".db", limit + 1),
            "trivial",
        )

    def test_generic_hidden_unknown_file_depends_on_size(self):
        limit = stackcopy.TRIVIAL_OTHER_FILE_BYTES
        self.assertEqual(
            stackcopy.classify_other_source_file(".foo", "", 100),
            "trivial",
        )
        self.assertEqual(
            stackcopy.classify_other_source_file(".foo", "", limit + 1),
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


class _UnreadableEntry:
    """Stand-in for an os.DirEntry whose stat() fails during scanning."""

    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path)

    def stat(self, *_args, **_kwargs):
        raise OSError("size cannot be determined")

    def is_file(self, *_args, **_kwargs):
        return True

    def is_dir(self, *_args, **_kwargs):
        return False


class PlanJsonStatFailureTests(unittest.TestCase):
    def test_unreadable_file_is_surfaced_as_data_not_trivial(self):
        real_iter = stackcopy.iter_source_file_entries

        def fake_iter(src_dir, *args, **kwargs):
            for entry in real_iter(src_dir, *args, **kwargs):
                if entry.name == "corrupt.dat":
                    yield _UnreadableEntry(entry.path)
                else:
                    yield entry

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            folder = card / "DCIM" / "100OLYMP"
            folder.mkdir(parents=True)
            (folder / "P8081885.ORF").write_bytes(b"0" * 9_000_000)
            # A ".dat" file that would be classed "trivial" at size 0, but whose
            # size can't be read: it must fail closed to "data".
            (folder / "corrupt.dat").write_bytes(b"0" * 10)

            env_keys = (
                "STACKCOPY_LIGHTROOM_IMPORT_DIR",
                "STACKCOPY_STACK_INPUT_DIR",
            )
            saved_env = {k: os.environ.get(k) for k in env_keys}
            os.environ["STACKCOPY_LIGHTROOM_IMPORT_DIR"] = str(root / "Lightroom")
            os.environ["STACKCOPY_STACK_INPUT_DIR"] = str(root / "StackInput")
            saved_argv = sys.argv
            saved_scan_iter = stackcopy.iter_source_file_entries
            saved_stack_dir = stackcopy.STACK_INPUT_DIR
            stdout = io.StringIO()
            try:
                stackcopy.iter_source_file_entries = fake_iter
                stackcopy.STACK_INPUT_DIR = str(root / "StackInput")
                sys.argv = [
                    "stackcopy.py",
                    "--lightroomimport",
                    str(card),
                    "--plan-json",
                    "--no-stack-detection",
                    "--dry-run",
                ]
                with contextlib.redirect_stdout(stdout):
                    try:
                        stackcopy.main()
                    except SystemExit as exc:
                        self.assertIn(exc.code, (None, 0))
            finally:
                stackcopy.iter_source_file_entries = saved_scan_iter
                stackcopy.STACK_INPUT_DIR = saved_stack_dir
                sys.argv = saved_argv
                for key, value in saved_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["other_files"], 1)
        self.assertEqual(payload["other_files_trivial"], 0)
        self.assertIn("corrupt.dat", payload["other_file_examples"])


if __name__ == "__main__":
    unittest.main()
