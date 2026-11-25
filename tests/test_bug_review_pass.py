"""Regression tests for the bugs fixed in Stackcopy 1.6.4.

Each class names the bug it pins down.  Display-free throughout: GUI methods
are exercised on stand-in objects, never a real window.
"""

from __future__ import annotations

import io
import json
import os
import queue
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import stackcopy  # noqa: E402
import stackcopy_gui_bootstrap as bootstrap  # noqa: E402
import stackcopy_updater as updater  # noqa: E402
from test_final_reliability_pass import make_pair, run_main, write_file  # noqa: E402
from test_gui_plan_helpers import plan_payload  # noqa: E402
from test_gui_update_state import gui  # noqa: E402


def tree(root: Path) -> set[str]:
    return (
        {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
        if root.exists()
        else set()
    )


def make_heuristic_stack(card: Path, start: datetime, frames: int = 3) -> None:
    """RAW+JPG frames 1..N, then a JPG-only stacked output, then a single."""
    for number in range(1, frames + 1):
        make_pair(card, f"P808{number:04d}", start + timedelta(seconds=number))
    write_file(
        card / f"P808{frames + 1:04d}.JPG",
        b"stacked output",
        start + timedelta(seconds=frames + 5),
    )
    make_pair(card, f"P808{frames + 2:04d}", start + timedelta(minutes=10))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class RecoveryKeepsCompanionsTogetherTests(unittest.TestCase):
    """The recovery pass filed each file under its own mtime's date."""

    def test_recovered_frame_keeps_raw_and_jpg_in_one_dated_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            # Frame 1's RAW is 23:59:59.9 on the 24th and its JPG 00:00:00.05
            # on the 25th; the planner files both under the RAW's date.
            write_file(card / "P8080001.ORF", b"r1", datetime(2026, 8, 24, 23, 59, 59, 900000))
            write_file(card / "P8080001.JPG", b"j1", datetime(2026, 8, 25, 0, 0, 0, 50000))
            for number in (2, 3):
                when = datetime(2026, 8, 25, 0, 0, number)
                make_pair(card, f"P808000{number}", when)
            write_file(card / "P8080004.JPG", b"out", datetime(2026, 8, 25, 0, 0, 8))

            real = stackcopy.safe_file_operation

            def stack_input_tree_is_broken(operation, src, dst, *args, **kwargs):
                if "StackInput" in dst:
                    return stackcopy.OperationResult(stackcopy.OperationOutcome.FAILED), 0
                return real(operation, src, dst, *args, **kwargs)

            _code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy,
                        "safe_file_operation",
                        side_effect=stack_input_tree_is_broken,
                    ),
                ),
            )

            placed = tree(root / "Lightroom")
            self.assertIn("2026/2026-08-24/P8080001.ORF", placed, output)
            self.assertIn("2026/2026-08-24/P8080001.JPG", placed, output)


class InteractivePromptEofTests(unittest.TestCase):
    """Ctrl-D at the -i prompt crashed with an EOFError traceback."""

    def test_end_of_input_is_treated_as_no(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            make_pair(card, "P8080001", datetime(2026, 8, 24, 12))

            code, output = run_main(
                ["--lightroomimport", str(card), "-i"],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
                extra_contexts=(
                    mock.patch.object(sys.stdin, "isatty", return_value=True),
                    mock.patch("builtins.input", side_effect=EOFError),
                ),
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Aborted.", output)
            self.assertEqual(tree(card), {"P8080001.JPG", "P8080001.ORF"})
            self.assertFalse((root / "Lightroom").exists())


class AlreadyProcessedNamesTests(unittest.TestCase):
    """'\\bstacked\\b' missed Stackcopy's own "stacked__2" collision names."""

    def test_stackcopy_outputs_are_recognized_with_or_without_a_suffix(self):
        for name in (
            "P8081888 stacked.JPG",
            "P8081888 stacked__2.JPG",
            "P8081888 Jackson State Forest stacked__3.jpg",
            "stacked_image.jpg",
            "image_stacked.jpg",
            "stacked-photo.jpg",
        ):
            with self.subTest(name=name):
                self.assertTrue(stackcopy.is_already_processed(name))

    def test_other_words_containing_stacked_are_not(self):
        for name in ("unstacked.jpg", "stackedness.jpg", "P8081888.JPG"):
            with self.subTest(name=name):
                self.assertFalse(stackcopy.is_already_processed(name))

    def test_rename_does_not_rename_a_suffixed_output_a_second_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "dated"
            write_file(folder / "P8081888 stacked__2.JPG", b"out", datetime(2026, 8, 24, 12))

            code, output = run_main(
                ["--rename", str(folder)],
                lightroom=Path(tmp) / "Lightroom",
                stack_input=Path(tmp) / "StackInput",
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(tree(folder), {"P8081888 stacked__2.JPG"})


class AppleDoubleFilesTests(unittest.TestCase):
    """A Mac-touched card's "._" metadata files were imported as photos."""

    def make_card(self, root: Path) -> Path:
        card = root / "card"
        folder = card / "DCIM" / "100OMSYS"
        when = datetime(2026, 8, 24, 12)
        make_pair(folder, "P8080001", when)
        for name in ("._P8080001.JPG", "._P8080001.ORF", "._P8080002.JPG"):
            write_file(folder / name, b"\x00\x05\x16\x07 appledouble", when)
        return card

    def test_appledouble_files_are_left_on_the_card_and_not_paired(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = self.make_card(root)

            code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(
                tree(root / "Lightroom"),
                {"2026/2026-08-24/P8080001.JPG", "2026/2026-08-24/P8080001.ORF"},
            )
            self.assertEqual(
                tree(card),
                {
                    "DCIM/100OMSYS/._P8080001.JPG",
                    "DCIM/100OMSYS/._P8080001.ORF",
                    "DCIM/100OMSYS/._P8080002.JPG",
                },
            )
            # A "._" JPG with no "._" RAW beside it is not a JPG-only folder.
            self.assertNotIn("JPG-only import detected", output)
            self.assertIn(stackcopy.APPLEDOUBLE_LABEL, output)

    def test_metadata_is_never_read_from_appledouble_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = self.make_card(root)
            seen = []

            def reader(paths):
                seen.extend(paths)
                return {}

            with mock.patch.object(
                stackcopy, "read_stacked_image_metadata", side_effect=reader
            ):
                run_main(
                    ["--lightroomimport", str(card), "--dry"],
                    lightroom=root / "Lightroom",
                    stack_input=root / "StackInput",
                )

            self.assertEqual([Path(p).name for p in seen], ["P8080001.JPG"])

    def test_plan_counts_them_as_trivial_and_the_card_still_empties(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = self.make_card(root)
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
                ],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads(completed.stdout)

            self.assertEqual(payload["total"], 2)
            self.assertEqual(payload["other_files"], 0)
            self.assertEqual(payload["other_files_trivial"], 3)
            self.assertTrue(payload["source_would_be_empty_after"])


class CaseInsensitiveStemTests(unittest.TestCase):
    """Since v1.1 only extensions were case-folded, contrary to the README."""

    def test_a_jpg_pairs_with_a_raw_whose_name_differs_only_in_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "photos"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "IMG_1234.JPG", b"jpg", when)
            write_file(source / "img_1234.orf", b"raw", when)
            write_file(source / "P0001.JPG", b"orphan", when)

            code, output = run_main(
                ["--copy", str(source), str(Path(tmp) / "out")],
                lightroom=Path(tmp) / "Lightroom",
                stack_input=Path(tmp) / "StackInput",
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(tree(Path(tmp) / "out"), {"P0001.JPG"})

    def test_an_import_keeps_each_files_own_spelling_and_one_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            write_file(card / "P8080001.JPG", b"j", datetime(2026, 8, 25, 0, 0, 0, 50000))
            write_file(card / "p8080001.orf", b"r", datetime(2026, 8, 24, 23, 59, 59, 900000))

            code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )

            self.assertEqual(code, 0, output)
            # One photo: both files follow the RAW's date, as for P1.JPG+P1.ORF.
            self.assertEqual(
                tree(root / "Lightroom"),
                {"2026/2026-08-24/P8080001.JPG", "2026/2026-08-24/p8080001.orf"},
            )

    def test_two_jpgs_differing_only_in_case_are_ambiguous_and_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            write_file(card / "P8080001.JPG", b"one", datetime(2026, 8, 24, 12))
            write_file(card / "p8080001.jpg", b"two", datetime(2026, 8, 24, 12))
            if len(list(card.iterdir())) != 2:
                self.skipTest("case-insensitive filesystem")

            code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )

            self.assertEqual(code, 1, output)
            self.assertIn("multiple JPG files share stem", output)
            self.assertEqual(tree(card), {"P8080001.JPG", "p8080001.jpg"})

    def test_a_previous_roll_under_a_different_parent_is_not_borrowed(self):
        # dcim/100OLYMP is not the roll before DCIM/101OLYMP: on a
        # case-sensitive disk they sit under different parents.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            (card / "dcim").mkdir(parents=True)
            if (card / "DCIM").exists():
                self.skipTest("case-insensitive filesystem")
            base = datetime(2026, 8, 24, 12)
            for index, stem in enumerate(("p8081866", "p8081867")):
                make_pair(card / "dcim" / "100OLYMP", stem, base.replace(second=index * 2))
            make_pair(card / "DCIM" / "101OLYMP", "P8081868", base.replace(second=4))
            write_file(
                card / "DCIM" / "101OLYMP" / "P8081869.JPG",
                b"stack output",
                base.replace(second=6),
            )

            code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )

            self.assertEqual(code, 0, output)
            borrowed = {
                name for name in tree(root / "StackInput") if "p808186" in name
            }
            self.assertEqual(borrowed, set(), output)

    def test_the_real_previous_roll_is_found_beside_a_case_variant_parent(self):
        # dcim/100OLYMP and DCIM/100OLYMP fold to the same path; the one under
        # DCIM must still be found as the roll before DCIM/101OLYMP.
        for unrelated_first in (True, False):
            with self.subTest(unrelated_first=unrelated_first), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                card = root / "card"
                (card / "dcim").mkdir(parents=True)
                if (card / "DCIM").exists():
                    self.skipTest("case-insensitive filesystem")
                base = datetime(2026, 8, 24, 12)
                rolls = [
                    (card / "dcim" / "100OLYMP", "p", base.replace(hour=9)),
                    (card / "DCIM" / "100OLYMP", "P", base),
                ]
                if not unrelated_first:
                    rolls.reverse()
                for folder, prefix, start in rolls:
                    for index, number in enumerate((1866, 1867)):
                        make_pair(
                            folder, f"{prefix}808{number}", start.replace(second=index * 2)
                        )
                make_pair(card / "DCIM" / "101OLYMP", "P8081868", base.replace(second=4))
                write_file(
                    card / "DCIM" / "101OLYMP" / "P8081869.JPG",
                    b"stack output",
                    base.replace(second=6),
                )

                code, output = run_main(
                    ["--lightroomimport", str(card)],
                    lightroom=root / "Lightroom",
                    stack_input=root / "StackInput",
                    metadata={},
                )

                self.assertEqual(code, 0, output)
                day = "2026/2026-08-24"
                self.assertEqual(
                    tree(root / "StackInput"),
                    {
                        f"{day}/P808{number}.{ext}"
                        for number in (1866, 1867, 1868)
                        for ext in ("JPG", "ORF")
                    },
                    output,
                )

    def test_a_stack_is_found_when_its_frames_prefix_case_differs_from_the_output(self):
        # Lower-case JPG frames sort first and name their records "p...",
        # while the stacked output is "P...": one sequence all the same.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            start = datetime(2026, 8, 24, 12)
            for number in range(1, 4):
                when = start + timedelta(seconds=number)
                write_file(card / f"p808000{number}.JPG", b"j", when)
                write_file(card / f"P808000{number}.ORF", b"r", when)
            write_file(card / "P8080004.JPG", b"out", start + timedelta(seconds=8))
            make_pair(card, "P8080006", start + timedelta(minutes=10))

            code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("JPG-only import detected", output)
            day = "2026/2026-08-24"
            self.assertEqual(
                tree(root / "StackInput"),
                {
                    f"{day}/{name}"
                    for number in range(1, 4)
                    for name in (f"p808000{number}.JPG", f"P808000{number}.ORF")
                },
            )
            self.assertEqual(
                tree(root / "Lightroom"),
                {
                    f"{day}/P8080004 stacked.JPG",
                    f"{day}/P8080006.JPG",
                    f"{day}/P8080006.ORF",
                },
            )


class SystemFoldersTests(unittest.TestCase):
    """Trashed photos were imported; OS index folders kept a card 'not empty'."""

    def make_card(self, root: Path) -> Path:
        card = root / "card"
        when = datetime(2026, 8, 24, 12)
        make_pair(card / "DCIM" / "100OMSYS", "P8080001", when)
        write_file(card / ".Trashes" / "501" / "P8080002.JPG", b"deleted", when)
        write_file(card / ".Trash-1000" / "files" / "P8080003.JPG", b"deleted", when)
        write_file(card / ".Spotlight-V100" / "Store-V2" / "index", b"x" * 2_000_000, when)
        write_file(card / ".fseventsd" / "fseventsd-uuid", b"x", when)
        write_file(card / "System Volume Information" / "IndexerVolumeGuid", b"x", when)
        return card

    def test_trash_and_index_folders_are_not_imported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = self.make_card(root)

            code, output = run_main(
                ["--lightroomimport", str(card)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(
                tree(root / "Lightroom"),
                {"2026/2026-08-24/P8080001.JPG", "2026/2026-08-24/P8080001.ORF"},
            )
            self.assertIn(".Trashes/501/P8080002.JPG", tree(card))
            self.assertIn(".Trash-1000/files/P8080003.JPG", tree(card))
            self.assertNotIn("JPG-only import detected", output)

    def test_they_do_not_stop_the_card_counting_as_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            card = self.make_card(Path(tmp))
            moved = [
                card / "DCIM" / "100OMSYS" / "P8080001.JPG",
                card / "DCIM" / "100OMSYS" / "P8080001.ORF",
            ]
            self.assertTrue(stackcopy.card_would_be_empty_after(str(card), moved))
            self.assertFalse(stackcopy.card_would_be_empty_after(str(card), moved[:1]))

    def test_folder_names_are_recognized_in_any_case(self):
        for name in (".Trashes", ".trash-1000", "$RECYCLE.BIN", "System Volume Information"):
            with self.subTest(name=name):
                self.assertTrue(stackcopy.is_system_dir(name))
        for name in ("DCIM", "100OMSYS", "Trash photos", "MISC"):
            with self.subTest(name=name):
                self.assertFalse(stackcopy.is_system_dir(name))


class ExifToolUnreportedFilesTests(unittest.TestCase):
    """A file ExifTool answered for under another name was dropped silently."""

    def test_an_unreported_file_warns_even_when_exiftool_exits_zero(self):
        requested = "/camera/Jörg/P8081868.JPG"
        # ExifTool on Windows echoes a non-ASCII folder name back with "?".
        stdout = json.dumps([{"SourceFile": "/camera/J?rg/P8081868.JPG"}])
        output = io.StringIO()
        with (
            mock.patch.object(
                stackcopy.subprocess,
                "run",
                return_value=SimpleNamespace(returncode=0, stdout=stdout),
            ),
            redirect_stdout(output),
        ):
            results = stackcopy.read_stacked_image_metadata([requested])

        self.assertEqual(results[requested].state, stackcopy.StackMetadataState.UNKNOWN)
        self.assertIn("could not be read for 1 file(s)", output.getvalue())
        self.assertIn("did not report", output.getvalue())

    def test_a_healthy_file_without_the_tag_is_still_quiet(self):
        path = "/camera/P8081868.JPG"
        output = io.StringIO()
        with (
            mock.patch.object(
                stackcopy.subprocess,
                "run",
                return_value=SimpleNamespace(
                    returncode=0, stdout=json.dumps([{"SourceFile": path}])
                ),
            ),
            redirect_stdout(output),
        ):
            stackcopy.read_stacked_image_metadata([path])

        self.assertEqual(output.getvalue(), "")


class GuiOutputEncodingTests(unittest.TestCase):
    """On Windows a piped stdout is cp1252/strict: non-ANSI paths crashed."""

    def test_a_non_ansi_destination_does_not_crash_a_gui_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            make_pair(card, "P8080001", datetime(2026, 8, 24, 12))
            lightroom = root / "张伟" / "Lightroom"
            env = dict(
                os.environ,
                # What a Windows pipe gets by default.
                PYTHONIOENCODING="cp1252",
                STACKCOPY_PROGRESS="1",
                STACKCOPY_LIGHTROOM_IMPORT_DIR=str(lightroom),
                STACKCOPY_STACK_INPUT_DIR=str(root / "StackInput"),
            )
            completed = subprocess.run(
                [sys.executable, str(ROOT / "stackcopy.py"), "--lightroomimport", str(card)],
                capture_output=True,
                env=env,
                check=False,
            )

            stdout = completed.stdout.decode("utf-8")
            self.assertEqual(
                completed.returncode, 0, stdout + completed.stderr.decode("utf-8", "replace")
            )
            self.assertIn("张伟", stdout)
            self.assertEqual(
                tree(lightroom),
                {"2026/2026-08-24/P8080001.JPG", "2026/2026-08-24/P8080001.ORF"},
            )


class StopAfterThisFileTests(unittest.TestCase):
    """The GUI's stop killed the CLI mid-file; it now asks via a stop file."""

    def run_import(self, root: Path, stop_file: Path, extra_contexts=()):
        return run_main(
            ["--lightroomimport", str(root / "card")],
            lightroom=root / "Lightroom",
            stack_input=root / "StackInput",
            metadata={},
            extra_contexts=(
                mock.patch.dict(os.environ, {stackcopy.STOP_FILE_ENV: str(stop_file)}),
                *extra_contexts,
            ),
        )

    def make_card(self, root: Path) -> None:
        card = root / "card"
        for number in range(1, 4):
            make_pair(card, f"P808{number:04d}", datetime(2026, 8, 24, 12, number))

    def test_a_stop_before_the_first_file_moves_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_card(root)
            stop_file = root / "stop"
            stop_file.touch()

            code, output = self.run_import(root, stop_file)

            self.assertEqual(code, 130, output)
            self.assertIn("Import stopped as requested", output)
            self.assertEqual(len(tree(root / "card")), 6)
            self.assertEqual(tree(root / "Lightroom"), set())

    def test_a_stop_finishes_the_current_file_and_starts_no_other(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_card(root)
            stop_file = root / "stop"
            real = stackcopy.safe_file_operation
            calls = {"n": 0}

            def stop_during_second_file(*args, **kwargs):
                calls["n"] += 1
                if calls["n"] == 2:
                    stop_file.touch()  # the user clicks Stop mid-file
                return real(*args, **kwargs)

            code, output = self.run_import(
                root,
                stop_file,
                (
                    mock.patch.object(
                        stackcopy,
                        "safe_file_operation",
                        side_effect=stop_during_second_file,
                    ),
                ),
            )

            self.assertEqual(code, 130, output)
            self.assertEqual(calls["n"], 2)
            self.assertEqual(len(tree(root / "Lightroom")), 2)
            self.assertEqual(len(tree(root / "card")), 4)
            self.assertIn("Completed: 2; failed: 0; remaining: 4.", output)
            leftovers = [
                name for name in tree(root) if stackcopy._TEMP_PREFIX in name
            ]
            self.assertEqual(leftovers, [])

    def test_a_stop_during_recovery_finishes_that_file_and_recovers_no_more(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            make_heuristic_stack(card, datetime(2026, 8, 24, 12))
            stop_file = root / "stop"
            real = stackcopy.safe_file_operation
            recovery_calls = []

            def stack_input_fails_then_stop_mid_recovery(
                operation, src, dst, label, *args, **kwargs
            ):
                if "StackInput" in dst:
                    return stackcopy.OperationResult(stackcopy.OperationOutcome.FAILED), 0
                if "recovered" in label:
                    recovery_calls.append(src)
                    stop_file.touch()  # the user clicks Stop mid-recovery
                return real(operation, src, dst, label, *args, **kwargs)

            code, output = self.run_import(
                root,
                stop_file,
                (
                    mock.patch.object(
                        stackcopy,
                        "safe_file_operation",
                        side_effect=stack_input_fails_then_stop_mid_recovery,
                    ),
                ),
            )

            self.assertEqual(code, 130, output)
            self.assertIn("Import stopped as requested", output)
            self.assertEqual(len(recovery_calls), 1, output)
            # Six stack inputs needed recovering; only the first was started.
            self.assertEqual(len(tree(card)), 5, output)

    def test_without_a_stop_file_the_import_runs_to_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_card(root)

            code, output = self.run_import(root, root / "never-created")

            self.assertEqual(code, 0, output)
            self.assertEqual(tree(root / "card"), set())


class StartEventTotalsTests(unittest.TestCase):
    """The "start" event now carries this run's own per-role totals."""

    def test_start_event_reports_what_this_run_will_do(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_heuristic_stack(root / "card", datetime(2026, 8, 24, 12))
            stderr = io.StringIO()
            with mock.patch.object(stackcopy, "_PROGRESS_ENABLED", True):
                code, output = run_main(
                    ["--lightroomimport", str(root / "card")],
                    lightroom=root / "Lightroom",
                    stack_input=root / "StackInput",
                    metadata={},
                    extra_contexts=(redirect_stderr(stderr),),
                )

            self.assertEqual(code, 0, output)
            start = next(
                line for line in stderr.getvalue().splitlines() if "phase=start" in line
            )
            fields, _name = gui.parse_progress(start)
            self.assertEqual(fields["stacks"], "1")
            self.assertEqual(fields["stacked_outputs"], "1")
            self.assertEqual(fields["stack_inputs"], "6")
            self.assertEqual(fields["others"], "2")


# ---------------------------------------------------------------------------
# Updater
# ---------------------------------------------------------------------------


class ReleaseUrlBrowserParsingTests(unittest.TestCase):
    """urlsplit() and a browser disagreed about "\\" and ".." segments."""

    def test_urls_a_browser_would_resolve_elsewhere_are_rejected(self):
        base = "https://github.com/AlanRockefeller/stackcopy/releases/tag/"
        for url in (
            base + "..\\..\\..\\..\\attacker\\evil\\releases",
            base + "%2e%2e\\%2e%2e\\%2e%2e\\%2e%2e\\attacker\\evil",
            base + "..",
            base + ".",
            base + "%2e%2e",
            base + "v1.6.0%2F..%2F..",
            # urlsplit() silently drops an embedded tab or newline.
            base + "v1.6\t.0",
            base + "v1\n/../../../attacker/evil",
            "https://github.com/AlanRockefeller\\stackcopy/releases",
        ):
            with self.subTest(url=url):
                self.assertFalse(updater.is_trusted_release_url(url))
                self.assertEqual(updater.safe_release_url(url), updater.RELEASES_URL)

    def test_real_release_tags_are_still_trusted(self):
        base = "https://github.com/AlanRockefeller/stackcopy/releases/tag/"
        for tag in ("v1.6.0", "v1.6.0-build2", "v2.0.0-rc.1", "1.7.0+mac"):
            with self.subTest(tag=tag):
                self.assertTrue(updater.is_trusted_release_url(base + tag))


class FutureTimestampTests(unittest.TestCase):
    """A stamp from the future silenced automatic checks until that date."""

    NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)

    def test_a_success_recorded_with_the_clock_years_ahead_does_not_silence_checks(self):
        state = updater.record_success({}, datetime(2031, 1, 1, tzinfo=timezone.utc))
        self.assertTrue(updater.should_check_automatically(state, self.NOW))

    def test_an_end_of_calendar_stamp_does_not_raise(self):
        for value in ("9999-12-31T23:30:00+00:00", 253402300799):
            with self.subTest(value=value):
                state = {updater.LAST_SUCCESS_KEY: value}
                self.assertTrue(updater.should_check_automatically(state, self.NOW))

    def test_ordinary_clock_skew_still_counts(self):
        state = updater.record_success({}, self.NOW + timedelta(minutes=2))
        self.assertFalse(updater.should_check_automatically(state, self.NOW))


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------


class ManualCheckDuringAutomaticCheckTests(unittest.TestCase):
    """A click while the startup check ran got no answer at all."""

    def test_the_click_is_answered_when_the_running_check_returns(self):
        events = []
        target = SimpleNamespace(
            _closing=False,
            _update_generation=1,
            _update_checking=True,
            _update_manual=False,
            _update_info=None,
            _state={},
            _updates_available=lambda: True,
            update_check_btn=SimpleNamespace(
                configure=lambda **kw: events.append(("button", kw))
            ),
            _save_current_defaults=lambda: None,
            _set_update_notice=lambda text: events.append(("notice", text)),
            _hide_update_notice=lambda: events.append(("hide", None)),
            _notify_update=lambda info: events.append(("notify", None)),
            _show_update_dialog=lambda: events.append(("dialog", None)),
        )
        gui.StackcopyGUI._check_for_updates_manually(target)
        self.assertIn(("button", {"state": "disabled"}), events)

        current = updater.UpdateInfo(
            current_version="1.6.3",
            latest_version="1.6.3",
            tag_name="v1.6.3",
            release_name="",
            release_url=updater.RELEASES_URL,
            published_at="",
            notes="",
            is_newer=False,
        )
        boxes = []
        fake_box = SimpleNamespace(
            showinfo=lambda *a: boxes.append("info"),
            showwarning=lambda *a: boxes.append("warning"),
        )
        with mock.patch.object(gui, "messagebox", fake_box):
            # The automatic check's own result arrives.
            gui.StackcopyGUI._handle_update_result(
                target,
                {"generation": 1, "manual": False, "info": current, "error": ""},
            )

        self.assertEqual(boxes, ["info"])
        self.assertNotIn(("hide", None), events)
        self.assertFalse(target._update_manual)


class UpdateCheckCertificateTests(unittest.TestCase):
    """The frozen Mac app had no CA bundle for GitHub's certificate."""

    def test_without_certifi_the_default_opener_is_used(self):
        with mock.patch.dict(sys.modules, {"certifi": None}):
            self.assertIsNone(gui.update_check_opener())

    def test_with_certifi_its_bundle_is_added_to_the_default_trust(self):
        import ssl

        cafile = ssl.get_default_verify_paths().openssl_cafile
        if not cafile or not os.path.isfile(cafile):
            self.skipTest("no CA bundle on this machine to stand in for certifi")
        fake_certifi = SimpleNamespace(where=lambda: cafile)
        with mock.patch.dict(sys.modules, {"certifi": fake_certifi}):
            opener = gui.update_check_opener()
        self.assertIsNotNone(opener)

        import urllib.request

        with mock.patch.object(urllib.request, "urlopen") as urlopen:
            opener("request", timeout=8)
        _args, kwargs = urlopen.call_args
        self.assertEqual(kwargs["timeout"], 8)
        self.assertIsInstance(kwargs["context"], ssl.SSLContext)
        self.assertEqual(kwargs["context"].verify_mode, ssl.CERT_REQUIRED)


class PendingPlanScanTests(unittest.TestCase):
    """A preview scan scheduled just before Start ran alongside the import."""

    def test_starting_an_import_cancels_a_scheduled_scan(self):
        window = mock.Mock()
        window._plan = None
        window._plan_after = "after#7"
        window._pending = (["--lightroomimport", "/card"], "/photos", "/stacks", False)
        window._assume_yes = False

        with mock.patch.object(gui.threading, "Thread") as thread:
            gui.StackcopyGUI._launch(window)

        window.after_cancel.assert_any_call("after#7")
        self.assertIsNone(window._plan_after)
        # The run is told where to look for a stop request.
        env = thread.call_args.kwargs["args"][1]
        self.assertEqual(env[gui.STOP_FILE_ENV], window._stop_file)
        self.assertFalse(os.path.exists(window._stop_file))

    def test_a_scan_that_fires_during_an_import_does_nothing(self):
        window = mock.Mock()
        window._running = True
        window._plan_after = "after#7"

        with mock.patch.object(gui.threading, "Thread") as thread:
            gui.StackcopyGUI._begin_plan_scan(window)

        thread.assert_not_called()
        window._set_plan_scanning.assert_not_called()


class StopButtonTests(unittest.TestCase):
    def window(self, stop_file: str, *, transfer_started: bool):
        window = mock.Mock()
        window._proc = mock.Mock()
        window._proc.poll.return_value = None
        window._stop_requested = False
        window._terminated_by_user = False
        window._stop_file = stop_file
        window._transfer_started_at = 12.0 if transfer_started else None
        window._request_stop_after_current_file = (
            lambda: gui.StackcopyGUI._request_stop_after_current_file(window)
        )
        return window

    def test_stop_during_the_transfer_asks_the_cli_instead_of_killing_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            stop_file = os.path.join(tmp, "stop")
            window = self.window(stop_file, transfer_started=True)

            gui.StackcopyGUI._on_cancel(window)

            self.assertTrue(os.path.exists(stop_file))
            window._terminate_process.assert_not_called()
            self.assertTrue(window._stop_requested)
            self.assertTrue(window._terminated_by_user)
            window.cancel_btn.configure.assert_called_once_with(state="disabled")

    def test_stop_falls_back_to_ending_the_run_if_the_request_cannot_be_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            unwritable = os.path.join(tmp, "gone", "stop")  # parent missing
            window = self.window(unwritable, transfer_started=True)

            gui.StackcopyGUI._on_cancel(window)

            window._terminate_process.assert_called_once_with(window._proc, "stop")
            self.assertFalse(window._stop_requested)

    def test_stop_during_the_scan_still_ends_it_straight_away(self):
        with tempfile.TemporaryDirectory() as tmp:
            stop_file = os.path.join(tmp, "stop")
            window = self.window(stop_file, transfer_started=False)

            gui.StackcopyGUI._on_cancel(window)

            window._terminate_process.assert_called_once_with(window._proc, "stop")
            self.assertFalse(os.path.exists(stop_file))

    def test_closing_mid_transfer_waits_for_the_stop_instead_of_breaking_the_pipe(self):
        with tempfile.TemporaryDirectory() as tmp:
            stop_file = os.path.join(tmp, "stop")
            window = self.window(stop_file, transfer_started=True)

            with mock.patch.object(gui.messagebox, "askyesno", return_value=True):
                gui.StackcopyGUI._on_close(window)

            self.assertTrue(os.path.exists(stop_file))
            self.assertTrue(window._close_when_stopped)
            window._terminate_process.assert_not_called()
            window._finish_close.assert_not_called()


class RunEndTests(unittest.TestCase):
    @staticmethod
    def window(**changes):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._terminated_by_user = False
        window._close_when_stopped = False
        window._proc = mock.Mock()
        window._low_space_report = None
        window._assume_yes = False
        window._log_lines = []
        window._total = 5
        window._degraded = False
        window._plan = plan_payload(bytes=8_600_000_000)
        window._started_at = time.perf_counter()
        window._progress_total_bytes = None
        window._pending = (["--lightroomimport", "/card"], "/photos", "/stacks", False)
        window.progress = mock.Mock()
        for name, value in changes.items():
            setattr(window, name, value)
        return window

    def test_quitting_after_a_requested_stop_closes_once_the_cli_has_stopped(self):
        window = self.window(_terminated_by_user=True, _close_when_stopped=True)

        gui.StackcopyGUI._handle_done(window, 130)

        window._finish_close.assert_called_once_with()
        window._show_result.assert_not_called()

    def test_a_stop_that_lands_after_the_last_file_is_a_success(self):
        window = self.window(_terminated_by_user=True)

        gui.StackcopyGUI._handle_done(window, 0)

        args, kwargs = window._show_result.call_args
        self.assertEqual(args[0], "5 files imported")
        self.assertTrue(kwargs["success"])

    def test_success_speed_uses_this_runs_bytes_not_the_whole_cards(self):
        window = self.window(_progress_total_bytes=2048)

        gui.StackcopyGUI._handle_done(window, 0)

        metrics = window._show_result.call_args.args[1]
        self.assertIn(gui.format_bytes(2048), metrics)
        self.assertNotIn(gui.format_bytes(8_600_000_000), metrics)

    def test_a_preview_never_asks_to_proceed_despite_low_space(self):
        window = self.window(
            _low_space_report={"dry_run": True, "destination": "/photos"},
            _pending=(["--lightroomimport", "/card", "--dry"], "/photos", "/stacks", True),
        )

        with mock.patch.object(gui.messagebox, "askyesno") as ask:
            gui.StackcopyGUI._handle_done(window, 1)

        ask.assert_not_called()
        self.assertEqual(window._show_result.call_args.args[0], "Preview did not finish")


class ContinuedImportCountersTests(unittest.TestCase):
    """Continuing an import counted against the plan of the whole card."""

    def test_counters_use_this_runs_totals_once_the_cli_reports_them(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window._plan = plan_payload(stacked_outputs=12, stack_inputs=96, others=234)
        window._run_bucket_totals = None
        self.assertEqual(gui.StackcopyGUI._bucket_total(window, "stack_input"), 96)

        window._run_bucket_totals = {"stack_output": 2, "stack_input": 10, "other": 40}
        self.assertEqual(gui.StackcopyGUI._bucket_total(window, "stack_output"), 2)
        self.assertEqual(gui.StackcopyGUI._bucket_total(window, "stack_input"), 10)
        self.assertEqual(gui.StackcopyGUI._bucket_total(window, "other"), 40)

    def test_the_start_event_is_where_the_run_totals_come_from(self):
        window = mock.Mock(spec=gui.StackcopyGUI)
        window.progress = mock.Mock()
        window.phase_var = mock.Mock()
        gui.StackcopyGUI._handle_progress(
            window,
            "@@SCPROGRESS phase=start done=0 total=52 stacks=2 "
            "stacked_outputs=2 stack_inputs=10 others=40",
        )
        self.assertEqual(
            window._run_bucket_totals,
            {"stack_output": 2, "stack_input": 10, "other": 40},
        )
        self.assertEqual(window._run_stacks, 2)


class QueueDrainTests(unittest.TestCase):
    """One failing handler stopped every later progress line for good."""

    def test_the_drain_reschedules_itself_even_when_a_handler_raises(self):
        window = mock.Mock()
        window._closing = False
        window._queue = queue.Queue()
        window._queue.put(("done", 0))
        window._handle_done.side_effect = RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            gui.StackcopyGUI._drain_queue(window)

        window.after.assert_called_once_with(100, window._drain_queue)

    def test_a_handler_that_closes_the_window_ends_the_drain(self):
        # Quitting after a requested stop closes the window from _handle_done;
        # nothing queued behind it may run, and Tk must not be rescheduled.
        window = mock.Mock()
        window._closing = False
        window._queue = queue.Queue()
        window._queue.put(("done", 130))
        window._queue.put(("out", "a line printed after the stop\n"))

        def close(_returncode):
            window._closing = True

        window._handle_done.side_effect = close

        gui.StackcopyGUI._drain_queue(window)

        window._log_write.assert_not_called()
        window.after.assert_not_called()


class BootstrapDialogTests(unittest.TestCase):
    """zenity read the message as Pango markup; "&" or "<" garbled it."""

    def test_zenity_shows_the_message_as_plain_text(self):
        with (
            mock.patch.object(bootstrap.os, "name", "posix"),
            mock.patch.object(bootstrap.sys, "platform", "linux"),
            mock.patch.object(
                bootstrap.shutil,
                "which",
                side_effect=lambda name: "/usr/bin/zenity" if name == "zenity" else None,
            ),
            mock.patch.object(bootstrap.subprocess, "run") as run,
        ):
            self.assertTrue(bootstrap._native_dialog("Title", "R&D <folder>"))

        args = run.call_args.args[0]
        self.assertIn("--no-markup", args)
        self.assertIn("--text=R&D <folder>", args)


if __name__ == "__main__":
    unittest.main()
