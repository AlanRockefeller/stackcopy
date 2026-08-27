"""Regression coverage for the final 1.6.0 reliability pass.

Each class maps to one reviewed finding.  Nothing here needs ExifTool, a real
read-only card, root access, or a particular filesystem: the subprocess and
filesystem boundaries are mocked, everything else is a real import run against
a temporary directory.
"""

import errno
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402

try:  # The GUI needs customtkinter, which is optional for the CLI test suite.
    import stackcopy_gui  # noqa: E402
except ImportError:  # pragma: no cover - depends on the local environment
    stackcopy_gui = None


UNKNOWN = stackcopy.StackMetadata(stackcopy.StackMetadataState.UNKNOWN)


def write_file(path: Path, content: bytes, when: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = when.timestamp()
    os.utime(path, (timestamp, timestamp))


def files_under(path: Path) -> dict[Path, bytes]:
    if not path.exists():
        return {}
    return {
        item.relative_to(path): item.read_bytes()
        for item in path.rglob("*")
        if item.is_file()
    }


def names_under(path: Path) -> set[str]:
    return {item.name for item in path.rglob("*") if item.is_file()}


def metadata_reader(values_by_name):
    def read(paths):
        return {path: values_by_name.get(Path(path).name, UNKNOWN) for path in paths}

    return read


def run_main(
    args: list[str],
    *,
    lightroom: Path,
    stack_input: Path,
    metadata=None,
    extra_contexts=(),
) -> tuple[int, str]:
    output = io.StringIO()
    exit_code = 0
    stackcopy._confirmed_filesystems.clear()
    environment = {
        "STACKCOPY_LIGHTROOM_IMPORT_DIR": str(lightroom),
        "STACKCOPY_ASSUME_YES": "1",
    }
    with ExitStack() as contexts:
        contexts.enter_context(mock.patch.dict(os.environ, environment, clear=False))
        contexts.enter_context(
            mock.patch.object(stackcopy, "STACK_INPUT_DIR", str(stack_input))
        )
        contexts.enter_context(mock.patch.object(sys, "argv", ["stackcopy.py", *args]))
        if metadata is not None:
            contexts.enter_context(
                mock.patch.object(
                    stackcopy,
                    "read_stacked_image_metadata",
                    side_effect=metadata_reader(metadata),
                )
            )
        for context in extra_contexts:
            contexts.enter_context(context)
        try:
            with redirect_stdout(output):
                stackcopy.main()
        except SystemExit as error:
            exit_code = error.code if isinstance(error.code, int) else 1
    return exit_code, output.getvalue()


def make_pair(source: Path, stem: str, when: datetime, extensions=("JPG", "ORF")):
    for extension in extensions:
        write_file(source / f"{stem}.{extension}", f"{stem}-{extension}".encode(), when)


# ---------------------------------------------------------------------------
# Finding 1 - a --force overwrite is never silent
# ---------------------------------------------------------------------------


class ForcedOverwriteVisibilityTests(unittest.TestCase):
    def make_conflict(self, root: Path) -> tuple[Path, Path, Path]:
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        write_file(source / "P8081868.ORF", b"new raw", datetime(2026, 8, 24, 12))
        write_file(
            lightroom / "2026" / "2026-08-24" / "P8081868.ORF",
            b"a different, older raw",
            datetime(2020, 1, 1, 9),
        )
        return source, lightroom, stack_input

    def broken_commit(self):
        """Fail the final commit while still letting the guard step work.

        --force quarantines the file it is about to replace before writing
        anything, so only the commit onto the real destination name may fail
        here; the announcement has already been printed by then.
        """
        real_rename = stackcopy.atomic_rename_no_replace

        def rename(src, dst):
            # Let the quarantine, and the restore that puts it back, work.
            if os.path.basename(os.fspath(src)).startswith(stackcopy._GUARD_PREFIX):
                return real_rename(src, dst)
            if os.path.basename(os.fspath(dst)) == "P8081868.ORF":
                raise OSError(errno.EIO, "device failure")
            return real_rename(src, dst)

        return mock.patch.object(
            stackcopy, "atomic_rename_no_replace", side_effect=rename
        )

    def test_dry_run_announces_the_overwrite_and_counts_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.make_conflict(Path(tmp))
            code, output = run_main(
                ["--lightroomimport", str(source), "--force", "--dry-run"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn(
                "Would overwrite differing existing file because --force:", output
            )
            self.assertIn("P8081868.ORF", output)
            self.assertIn(
                "Existing files that would be overwritten by --force: 1", output
            )
            # A dry run changes nothing at all.
            self.assertEqual(
                files_under(lightroom / "2026" / "2026-08-24"),
                {Path("P8081868.ORF"): b"a different, older raw"},
            )
            self.assertEqual(names_under(source), {"P8081868.ORF"})

    def test_real_run_announces_the_overwrite_without_verbose(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.make_conflict(Path(tmp))
            code, output = run_main(
                ["--lightroomimport", str(source), "--force"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn(
                "Overwriting differing existing file because --force:", output
            )
            self.assertIn("P8081868.ORF", output)
            self.assertIn("Existing files overwritten by --force: 1", output)
            self.assertEqual(
                files_under(lightroom / "2026" / "2026-08-24"),
                {Path("P8081868.ORF"): b"new raw"},
            )

    def test_identical_destinations_are_not_counted_as_force_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "P8081868.ORF", b"same bytes", when)
            write_file(
                lightroom / "2026" / "2026-08-24" / "P8081868.ORF", b"same bytes", when
            )

            code, output = run_main(
                ["--lightroomimport", str(source), "--force"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("because --force", output)
            self.assertNotIn("overwritten by --force", output)

    def test_zero_byte_repair_is_not_counted_as_a_force_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "P8081868.ORF", b"real data", datetime(2026, 8, 24, 12))
            write_file(
                lightroom / "2026" / "2026-08-24" / "P8081868.ORF",
                b"",
                datetime(2026, 8, 24, 12),
            )

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("exists but is 0 bytes", output)
            self.assertNotIn("overwritten by --force", output)

    def test_a_failed_copy_is_not_counted_as_an_overwrite(self):
        # The announcement is printed before the destructive step, but the
        # tally must reflect what actually happened: a copy that fails leaves
        # the pre-existing destination intact, so nothing was overwritten.
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.make_conflict(Path(tmp))

            code, output = run_main(
                ["--lightroomimport", str(source), "--force"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(self.broken_commit(),),
            )

            self.assertEqual(code, 1, output)
            self.assertIn(
                "Overwriting differing existing file because --force:", output
            )
            self.assertNotIn("Existing files overwritten by --force", output)
            self.assertIn("Failed to process 1 files.", output)
            # The pre-existing destination survived untouched.
            self.assertEqual(
                files_under(lightroom / "2026" / "2026-08-24"),
                {Path("P8081868.ORF"): b"a different, older raw"},
            )

    def test_same_run_collision_suffix_is_not_counted_as_a_force_overwrite(self):
        # Two source folders holding the same camera filename: the second one
        # gets a suffix.  --force must not turn that into an overwrite, and it
        # is not a pre-existing file, so it is not tallied either.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "100OMSYS" / "P8081868.ORF", b"first", when)
            write_file(source / "101OMSYS" / "P8081868.ORF", b"second", when)

            code, output = run_main(
                ["--lightroomimport", str(source), "--force"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("overwritten by --force", output)
            self.assertEqual(
                files_under(lightroom / "2026" / "2026-08-24"),
                {
                    Path("P8081868.ORF"): b"first",
                    Path("P8081868__2.ORF"): b"second",
                },
            )

    def test_force_never_lets_one_source_overwrite_another_from_this_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            for folder, content in (("100OMSYS", b"aaa"), ("101OMSYS", b"bbb")):
                write_file(source / folder / "P8081868.JPG", content, when)

            code, output = run_main(
                ["--lightroomimport", str(source), "--force"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            landed = files_under(lightroom / "2026" / "2026-08-24")
            self.assertEqual(sorted(landed.values()), [b"aaa", b"bbb"])


# ---------------------------------------------------------------------------
# Finding 2 - duplicate stem + logical file type
# ---------------------------------------------------------------------------


class DuplicateLogicalTypeTests(unittest.TestCase):
    def build(self, root: Path, duplicates: dict[str, bytes]):
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        when = datetime(2026, 8, 24, 12)
        for name, content in duplicates.items():
            write_file(source / name, content, when)
        # An unrelated neighbouring stem that must still import normally.
        write_file(source / "P8089999.JPG", b"neighbour", when)
        return source, lightroom, stack_input

    def assert_ambiguity_is_refused(self, args, duplicates, label, names):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.build(Path(tmp), duplicates)
            before = files_under(source)

            code, output = run_main(
                ["--lightroomimport", str(source), *args],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 1, output)
            self.assertNotIn("Traceback", output)
            self.assertIn(f"Error: multiple {label} files share stem", output)
            for name in names:
                self.assertIn(name, output)
            self.assertIn("Leaving this stem untouched.", output)
            self.assertIn("Ambiguous stems left untouched: 1", output)

            # Every affected physical file is still exactly where it was...
            remaining = files_under(source)
            for name in names:
                self.assertEqual(remaining[Path(name)], before[Path(name)])
            # ...and the unaffected neighbour still imported (or, in a dry run,
            # was still planned).
            dry = "--dry-run" in args
            self.assertEqual(
                Path("P8089999.JPG") in remaining,
                dry,
                output,
            )
            if not dry:
                self.assertIn("P8089999.JPG", names_under(lightroom))
            return output

    def test_orf_and_dng_sharing_a_stem_are_refused(self):
        self.assert_ambiguity_is_refused(
            (),
            {"P8081868.ORF": b"orf", "P8081868.DNG": b"dng"},
            "RAW",
            ["P8081868.ORF", "P8081868.DNG"],
        )

    def test_jpg_and_jpeg_sharing_a_stem_are_refused(self):
        self.assert_ambiguity_is_refused(
            (),
            {"P8081868.JPG": b"jpg", "P8081868.JPEG": b"jpeg"},
            "JPG",
            ["P8081868.JPG", "P8081868.JPEG"],
        )

    def test_mov_and_mp4_sharing_a_stem_are_refused(self):
        self.assert_ambiguity_is_refused(
            (),
            {"P8081868.MOV": b"mov", "P8081868.MP4": b"mp4"},
            "video",
            ["P8081868.MOV", "P8081868.MP4"],
        )

    def test_dry_run_reports_the_ambiguity_too(self):
        output = self.assert_ambiguity_is_refused(
            ("--dry-run",),
            {"P8081868.ORF": b"orf", "P8081868.DNG": b"dng"},
            "RAW",
            ["P8081868.ORF", "P8081868.DNG"],
        )
        self.assertIn("DRY RUN", output)

    def test_ambiguous_stem_cannot_join_a_stack(self):
        # P8080002 is ambiguous, so the backward walk from the output stops
        # there: nothing is filed as a stack and no file is moved into the
        # stack-input tree from that sequence.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base + timedelta(seconds=number))
            write_file(source / "P8080002.DNG", b"second raw", base)
            write_file(
                source / "P8080004.JPG", b"stack output", base + timedelta(seconds=4)
            )

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={
                    "P8080004.JPG": stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                    )
                },
            )

            self.assertEqual(code, 1, output)
            self.assertIn("Error: multiple RAW files share stem 'P8080002'", output)
            # Both ambiguous files stayed put.
            self.assertIn("P8080002.ORF", names_under(source))
            self.assertIn("P8080002.DNG", names_under(source))
            # The walk stopped at the gap, so P8080001 was never claimed.
            self.assertNotIn("P8080001.ORF", names_under(stack_input))

    def test_unrelated_import_without_duplicates_still_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            make_pair(source, "P8081868", datetime(2026, 8, 24, 12))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("Ambiguous stems", output)
            self.assertEqual(files_under(source), {})


# ---------------------------------------------------------------------------
# Finding 3 - durability barrier before deleting a cross-device source
# ---------------------------------------------------------------------------


class CrossDeviceDurabilityTests(unittest.TestCase):
    """Prove the ordering copy -> fsync file -> replace -> fsync dir -> unlink."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.src = root / "card" / "P8081868.ORF"
        self.dest = root / "dest" / "P8081868.ORF"
        write_file(self.src, b"irreplaceable", datetime(2026, 8, 24, 12))
        self.dest.parent.mkdir(parents=True, exist_ok=True)

    def instrumented(self, calls, *, fsync_file_error=None, directory_sync=None):
        """Patch the durability boundary and record the call order."""
        real_copyfile = stackcopy.shutil.copyfile
        real_rename = stackcopy.atomic_rename_no_replace
        real_unlink = stackcopy.os.unlink

        def copyfile(src, dst, **kwargs):
            calls.append("copy")
            return real_copyfile(src, dst, **kwargs)

        def fsync_file(_path):
            calls.append("fsync_file")
            if fsync_file_error is not None:
                raise fsync_file_error

        def fsync_directory(_path):
            calls.append("fsync_directory")
            return directory_sync or stackcopy.DirectorySync.SYNCED

        def rename(src, dst):
            # Only the temp -> final commit is interesting; the initial
            # fast-path rename is what raises EXDEV below.
            calls.append("replace")
            return real_rename(src, dst)

        def unlink(path, **kwargs):
            # Temp-file cleanup also goes through os.unlink; only the source
            # deletion is part of the ordering under test.
            if os.fspath(path) == str(self.src):
                calls.append("unlink_source")
            return real_unlink(path, **kwargs)

        return (
            mock.patch.object(stackcopy.shutil, "copyfile", side_effect=copyfile),
            mock.patch.object(stackcopy, "fsync_file", side_effect=fsync_file),
            mock.patch.object(
                stackcopy, "fsync_directory", side_effect=fsync_directory
            ),
            mock.patch.object(
                stackcopy, "atomic_rename_no_replace", side_effect=rename
            ),
            mock.patch.object(stackcopy.os, "unlink", side_effect=unlink),
        )

    def force_cross_device(self, calls):
        """Make the same-filesystem fast path report EXDEV."""
        real_rename = stackcopy.atomic_rename_no_replace
        state = {"first": True}

        def rename(src, dst):
            if state["first"] and os.fspath(dst) == str(self.dest):
                state["first"] = False
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            calls.append("replace")
            return real_rename(src, dst)

        return mock.patch.object(
            stackcopy, "atomic_rename_no_replace", side_effect=rename
        )

    def run_move(self, *, fsync_file_error=None, directory_sync=None):
        calls: list[str] = []
        patches = self.instrumented(
            calls,
            fsync_file_error=fsync_file_error,
            directory_sync=directory_sync,
        )
        # The instrumented commit is replaced by the EXDEV-raising one.
        patches = patches[:3] + (self.force_cross_device(calls), patches[4])
        output = io.StringIO()
        with ExitStack() as contexts:
            for patch in patches:
                contexts.enter_context(patch)
            with redirect_stdout(output):
                result, moved = stackcopy.safe_file_operation(
                    "move", str(self.src), str(self.dest), "moving test file"
                )
        return calls, result, moved, output.getvalue()

    def test_ordering_is_copy_fsync_replace_fsync_dir_unlink(self):
        calls, result, _moved, output = self.run_move()

        self.assertEqual(
            calls,
            ["copy", "fsync_file", "replace", "fsync_directory", "unlink_source"],
            output,
        )
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.SUCCESS
        )
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertFalse(self.src.exists())

    def test_file_fsync_failure_leaves_the_source_untouched(self):
        calls, result, _moved, output = self.run_move(
            fsync_file_error=OSError(errno.EIO, "I/O error")
        )

        self.assertEqual(calls, ["copy", "fsync_file"], output)
        self.assertNotIn("replace", calls)
        self.assertNotIn("unlink_source", calls)
        self.assertFalse(bool(result))
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.FAILED
        )
        self.assertIn("could not flush", output)
        self.assertIn("The source was left untouched.", output)
        # The source still holds the only copy, and no partial file was left.
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertFalse(self.dest.exists())
        self.assertEqual(list(self.dest.parent.iterdir()), [])

    def test_directory_fsync_failure_keeps_the_source(self):
        calls, result, _moved, output = self.run_move(
            directory_sync=stackcopy.DirectorySync.FAILED
        )

        self.assertEqual(
            calls, ["copy", "fsync_file", "replace", "fsync_directory"], output
        )
        self.assertNotIn("unlink_source", calls)
        # The destination is placed, so this is degraded rather than failed.
        self.assertTrue(bool(result))
        self.assertEqual(
            stackcopy.operation_outcome_of(result),
            stackcopy.OperationOutcome.COPIED_SOURCE_REMAINS,
        )
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")

    def test_platform_without_directory_fsync_still_completes_the_move(self):
        calls, result, _moved, output = self.run_move(
            directory_sync=stackcopy.DirectorySync.UNSUPPORTED
        )

        self.assertEqual(
            calls,
            ["copy", "fsync_file", "replace", "fsync_directory", "unlink_source"],
            output,
        )
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.SUCCESS
        )
        self.assertFalse(self.src.exists())

    def test_same_filesystem_move_does_not_fsync(self):
        with (
            mock.patch.object(stackcopy, "fsync_file") as fsync_file,
            mock.patch.object(stackcopy, "fsync_directory") as fsync_directory,
        ):
            result, _moved = stackcopy.safe_file_operation(
                "move", str(self.src), str(self.dest), "moving test file"
            )

        self.assertTrue(bool(result))
        fsync_file.assert_not_called()
        fsync_directory.assert_not_called()
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")

    def test_plain_copy_does_not_fsync_because_the_source_stays(self):
        with (
            mock.patch.object(stackcopy, "fsync_file") as fsync_file,
            mock.patch.object(stackcopy, "fsync_directory") as fsync_directory,
        ):
            result, _moved = stackcopy.safe_file_operation(
                "copy", str(self.src), str(self.dest), "copying test file"
            )

        self.assertTrue(bool(result))
        fsync_file.assert_not_called()
        fsync_directory.assert_not_called()
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertTrue(self.src.exists())

    def test_windows_reports_directory_fsync_as_unsupported(self):
        with mock.patch.object(stackcopy, "IS_WINDOWS", True):
            self.assertFalse(stackcopy.directory_fsync_supported())
            self.assertIs(
                stackcopy.fsync_directory(str(self.dest.parent)),
                stackcopy.DirectorySync.UNSUPPORTED,
            )

    def test_unsupported_errno_is_reported_as_unsupported_not_failure(self):
        stackcopy._directory_fsync_warning_shown = False
        self.addCleanup(setattr, stackcopy, "_directory_fsync_warning_shown", False)
        output = io.StringIO()
        with (
            mock.patch.object(stackcopy, "IS_WINDOWS", False),
            mock.patch.object(
                stackcopy.os, "fsync", side_effect=OSError(errno.EINVAL, "nope")
            ),
            redirect_stdout(output),
        ):
            result = stackcopy.fsync_directory(str(self.dest.parent))

        self.assertIs(result, stackcopy.DirectorySync.UNSUPPORTED)
        self.assertIn("cannot flush directory entries", output.getvalue())


# ---------------------------------------------------------------------------
# Finding 4 - one bad file must not discard a whole ExifTool batch
# ---------------------------------------------------------------------------


class ExifToolPartialBatchTests(unittest.TestCase):
    def read(self, paths, *, returncode=0, stdout="", side_effect=None):
        output = io.StringIO()
        run_kwargs = {}
        if side_effect is not None:
            run_kwargs["side_effect"] = side_effect
        else:
            run_kwargs["return_value"] = SimpleNamespace(
                returncode=returncode, stdout=stdout
            )
        with (
            mock.patch.object(
                stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
            ),
            mock.patch.object(stackcopy.subprocess, "run", **run_kwargs) as run,
            redirect_stdout(output),
        ):
            results = stackcopy.read_stacked_image_metadata(paths)
        return results, output.getvalue(), run

    def test_two_good_entries_survive_a_third_file_erroring(self):
        good_a, good_b, broken = (
            "/camera/P8081868.JPG",
            "/camera/P8081869.JPG",
            "/camera/P8081870.JPG",
        )
        payload = [
            {"SourceFile": good_a, "StackedImage": "9 8"},
            {"SourceFile": good_b, "StackedImage": "0 0"},
            {"SourceFile": broken, "Error": "File format error"},
        ]

        results, output, _run = self.read(
            [good_a, good_b, broken], returncode=1, stdout=json.dumps(payload)
        )

        self.assertEqual(
            results[good_a].state, stackcopy.StackMetadataState.FOCUS_STACK
        )
        self.assertEqual(results[good_a].frame_count, 8)
        self.assertEqual(
            results[good_b].state, stackcopy.StackMetadataState.NOT_FOCUS_STACK
        )
        # Metadata absence never becomes a negative answer.
        self.assertEqual(results[broken].state, stackcopy.StackMetadataState.UNKNOWN)
        self.assertIn("could not be read for 1 file(s)", output)

    def test_one_damaged_file_does_not_unknown_the_whole_batch(self):
        paths = [f"/camera/P808{number:04d}.JPG" for number in range(1, 251)]
        payload = [{"SourceFile": path, "StackedImage": "9 6"} for path in paths[:-1]]
        payload.append({"SourceFile": paths[-1], "Error": "Truncated file"})

        results, _output, _run = self.read(
            paths, returncode=1, stdout=json.dumps(payload)
        )

        confirmed = sum(
            1
            for value in results.values()
            if value.state == stackcopy.StackMetadataState.FOCUS_STACK
        )
        self.assertEqual(confirmed, 249)
        self.assertEqual(results[paths[-1]].state, stackcopy.StackMetadataState.UNKNOWN)

    def test_timeout_falls_back_safely_with_one_warning(self):
        paths = ["/camera/a.JPG", "/camera/b.JPG"]
        results, output, _run = self.read(
            paths,
            side_effect=stackcopy.subprocess.TimeoutExpired(
                cmd="exiftool", timeout=120
            ),
        )

        for value in results.values():
            self.assertEqual(value.state, stackcopy.StackMetadataState.UNKNOWN)
        self.assertIn("timed out", output)
        self.assertEqual(output.count("Warning: camera stack metadata"), 1)

    def test_malformed_json_falls_back_safely(self):
        paths = ["/camera/a.JPG"]
        results, output, _run = self.read(paths, returncode=0, stdout="not json{")

        self.assertEqual(results[paths[0]].state, stackcopy.StackMetadataState.UNKNOWN)
        self.assertIn("could not be parsed", output)

    def test_missing_exiftool_is_silent_and_unknown(self):
        output = io.StringIO()
        missing = stackcopy.ExifToolInfo(
            None, None, None, stackcopy.ExifToolSource.NONE
        )
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=missing),
            redirect_stdout(output),
        ):
            results = stackcopy.read_stacked_image_metadata(["/camera/a.JPG"])

        self.assertEqual(
            results["/camera/a.JPG"].state, stackcopy.StackMetadataState.UNKNOWN
        )
        self.assertEqual(output.getvalue(), "")

    def test_old_exiftool_without_the_tag_is_quiet(self):
        # An ExifTool too old to know Olympus:StackedImage simply omits it.
        # That is ordinary absence, not degradation, so it must not warn.
        path = "/camera/a.JPG"
        results, output, _run = self.read(
            [path], returncode=0, stdout=json.dumps([{"SourceFile": path}])
        )

        self.assertEqual(results[path].state, stackcopy.StackMetadataState.UNKNOWN)
        self.assertEqual(output, "")

    def test_subprocess_is_hardened(self):
        path = "/camera/a.JPG"
        _results, _output, run = self.read(
            [path], returncode=0, stdout=json.dumps([{"SourceFile": path}])
        )

        kwargs = run.call_args.kwargs
        self.assertIs(kwargs["stdin"], stackcopy.subprocess.DEVNULL)
        self.assertEqual(kwargs["encoding"], "utf-8")
        self.assertEqual(kwargs["errors"], "replace")
        self.assertFalse(kwargs.get("check", False))


# ---------------------------------------------------------------------------
# Finding 5 - adjacent-folder provenance
# ---------------------------------------------------------------------------


class AdjacentFolderProvenanceTests(unittest.TestCase):
    """A previous folder may extend a stack, never patch a hole in this one."""

    BASE = datetime(2026, 8, 24, 12)

    def run_import(self, source, lightroom, stack_input, metadata, args=()):
        return run_main(
            ["--lightroomimport", str(source), *args],
            lightroom=lightroom,
            stack_input=stack_input,
            metadata=metadata,
        )

    def dirs(self, root: Path):
        return root / "card", root / "Lightroom", root / "StackInput"

    def confirmed(self, count):
        return stackcopy.StackMetadata(
            stackcopy.StackMetadataState.FOCUS_STACK, count, f"9 {count}"
        )

    def test_legitimate_stack_crossing_folder_100_to_101(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            for number in (7, 8, 9):
                make_pair(
                    source / "100OMSYS",
                    f"P808{number:04d}",
                    self.BASE + timedelta(seconds=number),
                )
            write_file(
                source / "101OMSYS" / "P8080010.JPG",
                b"stack output",
                self.BASE + timedelta(seconds=11),
            )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                {"P8080010.JPG": self.confirmed(3)},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            # All three frames crossed the boundary into the stack-input tree.
            self.assertEqual(
                names_under(stack_input),
                {
                    f"P808{number:04d}.{extension}"
                    for number in (7, 8, 9)
                    for extension in ("JPG", "ORF")
                },
            )
            self.assertEqual(names_under(lightroom), {"P8080010 stacked.JPG"})

    def test_previous_folder_photo_does_not_fill_a_hole_in_this_folder(self):
        # 101OMSYS holds frames 5..8 and 10; frame 9 was deleted.  100OMSYS
        # happens to contain an old, unrelated P8080009.  It must not be
        # adopted as the missing component.
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            write_file(
                source / "100OMSYS" / "P8080009.JPG",
                b"old unrelated photo",
                datetime(2026, 8, 1, 9),
            )
            for number in (5, 6, 7, 8):
                make_pair(
                    source / "101OMSYS",
                    f"P808{number:04d}",
                    self.BASE + timedelta(seconds=number),
                )
            write_file(
                source / "101OMSYS" / "P8080010.JPG",
                b"stack output",
                self.BASE + timedelta(seconds=11),
            )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                {"P8080010.JPG": self.confirmed(5)},
            )

            self.assertEqual(code, 0, output)
            # The output is still a confirmed stack even though inputs are gone.
            self.assertIn("Accepted stacks:               1", output)
            self.assertIn("expected source frame(s) are unavailable", output)
            self.assertIn("P8080010 stacked.JPG", names_under(lightroom))
            # The unrelated previous-folder photo was imported as an ordinary
            # photo into its own date folder, not filed as a stack input.
            self.assertNotIn("P8080009.JPG", names_under(stack_input))
            self.assertIn(
                Path("2026") / "2026-08-01" / "P8080009.JPG",
                set(files_under(lightroom)),
            )

    def test_implausible_timestamp_blocks_a_lone_previous_folder_frame(self):
        # The narrow case folder provenance alone cannot settle: this folder
        # holds nothing but the output, so number 9 in the previous folder is
        # numerically a legitimate boundary crossing.  Its timestamp is days
        # away from the capture, so it is refused as a stack component.
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            write_file(
                source / "100OMSYS" / "P8080009.JPG",
                b"old unrelated photo",
                datetime(2026, 8, 1, 9),
            )
            write_file(
                source / "101OMSYS" / "P8080010.JPG",
                b"stack output",
                self.BASE + timedelta(seconds=11),
            )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                {"P8080010.JPG": self.confirmed(8)},
            )

            self.assertEqual(code, 0, output)
            self.assertIn(
                "not treating '100OMSYS/P8080009' from the adjacent camera folder "
                "as a stack input",
                output.replace(os.sep, "/"),
            )
            # The output is still a confirmed stack...
            self.assertIn("Accepted stacks:               1", output)
            self.assertIn("P8080010 stacked.JPG", names_under(lightroom))
            # ...and the old photo was imported as an ordinary photo.
            self.assertEqual(files_under(stack_input), {})
            self.assertIn(
                Path("2026") / "2026-08-01" / "P8080009.JPG",
                set(files_under(lightroom)),
            )

    def test_a_plausible_timestamp_still_crosses_the_folder_boundary(self):
        # The same shape as above, but captured seconds apart: accepted.
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            make_pair(source / "100OMSYS", "P8080009", self.BASE + timedelta(seconds=9))
            write_file(
                source / "101OMSYS" / "P8080010.JPG",
                b"stack output",
                self.BASE + timedelta(seconds=11),
            )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                {"P8080010.JPG": self.confirmed(1)},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("not treating", output)
            self.assertEqual(names_under(stack_input), {"P8080009.JPG", "P8080009.ORF"})

    def test_overlapping_numbering_refuses_to_borrow_from_the_previous_folder(self):
        # The camera's numbering reset: both folders cover the same numbers.
        # Nothing may be borrowed across the boundary at all.
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            # RAW-only frames, so the previous folder contributes no
            # stack-output candidate of its own and no clashing JPG name.
            for number in (8, 9, 10, 11):
                make_pair(
                    source / "100OMSYS",
                    f"P808{number:04d}",
                    datetime(2026, 8, 1, 9) + timedelta(seconds=number),
                    extensions=("ORF",),
                )
            for number in (8, 9):
                make_pair(
                    source / "101OMSYS",
                    f"P808{number:04d}",
                    self.BASE + timedelta(seconds=number),
                )
            write_file(
                source / "101OMSYS" / "P8080010.JPG",
                b"stack output",
                self.BASE + timedelta(seconds=11),
            )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                {"P8080010.JPG": self.confirmed(4)},
            )

            self.assertEqual(code, 0, output)
            # Only the current folder's own two frames became stack inputs.
            self.assertEqual(
                set(files_under(stack_input)),
                {
                    Path("2026") / "2026-08-24" / f"P808{number:04d}.{extension}"
                    for number in (8, 9)
                    for extension in ("JPG", "ORF")
                },
            )
            # Every previous-folder frame was imported as an ordinary photo
            # into its own date folder instead.
            landed = set(files_under(lightroom))
            for number in (8, 9, 10, 11):
                self.assertIn(
                    Path("2026") / "2026-08-01" / f"P808{number:04d}.ORF", landed
                )
            self.assertIn(Path("2026") / "2026-08-24" / "P8080010 stacked.JPG", landed)

    def test_missing_input_keeps_the_output_classified_as_a_stack(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            for number in (7, 8, 9):
                make_pair(
                    source, f"P808{number:04d}", self.BASE + timedelta(seconds=number)
                )
            write_file(
                source / "P8080010.JPG",
                b"stack output",
                self.BASE + timedelta(seconds=11),
            )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                # The camera says eight frames; only three survive.
                {"P8080010.JPG": self.confirmed(8)},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            self.assertIn("5 expected source frame(s) are unavailable", output)
            self.assertEqual(names_under(lightroom), {"P8080010 stacked.JPG"})

    def test_get_stack_sequence_rule_is_exercised_directly(self):
        # A focused check of the numeric rule, independent of any import run.
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.dirs(Path(tmp))
            # Previous folder strictly below -> borrowed.
            for number in (7, 8):
                make_pair(
                    source / "100OMSYS",
                    f"P808{number:04d}",
                    self.BASE + timedelta(seconds=number),
                )
            for number in (9, 10):
                make_pair(
                    source / "101OMSYS",
                    f"P808{number:04d}",
                    self.BASE + timedelta(seconds=number),
                )

            code, output = self.run_import(
                source,
                lightroom,
                stack_input,
                {"P8080010.JPG": self.confirmed(3)},
            )

            self.assertEqual(code, 0, output)
            # Frames 7, 8 and 9 were all reachable and became inputs.
            self.assertEqual(
                names_under(stack_input),
                {
                    f"P808{number:04d}.{extension}"
                    for number in (7, 8, 9)
                    for extension in ("JPG", "ORF")
                },
            )


# ---------------------------------------------------------------------------
# Finding 6 - recovery is a degraded outcome
# ---------------------------------------------------------------------------


class RecoveryIsDegradedTests(unittest.TestCase):
    def test_recovered_stack_input_reports_recovery_and_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base + timedelta(seconds=number))
            write_file(
                source / "P8080004.JPG", b"stack output", base + timedelta(seconds=5)
            )

            real_operation = stackcopy.safe_file_operation

            def flaky(operation, src, dest, description, *rest):
                # Every planned stack-input placement for P8080002 fails, so
                # the whole stem is recovered into the Lightroom hierarchy.
                if Path(src).stem == "P8080002" and "recovered" not in description:
                    return False, 0
                return real_operation(operation, src, dest, description, *rest)

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={
                    "P8080004.JPG": stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                    )
                },
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "safe_file_operation", side_effect=flaky
                    ),
                ),
            )

            self.assertEqual(code, 1, output)
            self.assertIn("Import completed with recovery.", output)
            self.assertIn("2 file(s) could not be placed as planned", output)
            self.assertIn("No data was lost, but manual review is recommended.", output)
            # Recovered files are not counted as unrecovered failures.
            self.assertIn("2 recovered; 0 unrecovered", output)
            self.assertIn("Failures: 0", output)
            # The data is safely at the fallback destination and gone from the
            # card, and the planned stack destination never received it.
            self.assertEqual(files_under(source), {})
            self.assertNotIn("P8080002.ORF", names_under(stack_input))
            self.assertIn("P8080002.ORF", names_under(lightroom))

    def test_completely_successful_import_still_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base + timedelta(seconds=number))
            write_file(
                source / "P8080004.JPG", b"stack output", base + timedelta(seconds=5)
            )

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={
                    "P8080004.JPG": stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                    )
                },
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("Import completed with recovery.", output)
            self.assertEqual(files_under(source), {})


# ---------------------------------------------------------------------------
# Finding 7 - copied, but the source could not be removed
# ---------------------------------------------------------------------------


class CopiedSourceRemainsTests(unittest.TestCase):
    def cross_device_unlink_failure(self, source: Path):
        """Force EXDEV on the fast path, then refuse to delete the source."""
        real_rename = stackcopy.atomic_rename_no_replace
        real_unlink = stackcopy.os.unlink
        seen: set[str] = set()

        def rename(src, dst):
            key = os.fspath(dst)
            if key not in seen and not os.path.basename(key).startswith(
                stackcopy._TEMP_PREFIX
            ):
                seen.add(key)
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            return real_rename(src, dst)

        def unlink(path, **kwargs):
            if str(source) in os.fspath(path):
                raise PermissionError(errno.EACCES, "read-only card")
            return real_unlink(path, **kwargs)

        return (
            mock.patch.object(
                stackcopy, "atomic_rename_no_replace", side_effect=rename
            ),
            mock.patch.object(stackcopy.os, "unlink", side_effect=unlink),
        )

    def build(self, root: Path):
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        write_file(source / "P8081868.ORF", b"irreplaceable", datetime(2026, 8, 24, 12))
        return source, lightroom, stack_input

    def test_summary_distinguishes_it_and_the_run_is_degraded(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.build(Path(tmp))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=self.cross_device_unlink_failure(source),
            )

            self.assertEqual(code, 1, output)
            self.assertIn("source could not be deleted", output)
            self.assertIn(
                "Copied successfully but source could not be removed: 1", output
            )
            self.assertIn("Imported normally: 0", output)
            self.assertIn("Failures: 0", output)
            self.assertNotIn("Import completed with recovery.", output)

            # The destination holds the exact bytes...
            self.assertEqual(
                files_under(lightroom / "2026" / "2026-08-24"),
                {Path("P8081868.ORF"): b"irreplaceable"},
            )
            # ...and the source is still there, un-duplicated.
            self.assertEqual(
                files_under(source), {Path("P8081868.ORF"): b"irreplaceable"}
            )

    def test_the_summary_totals_add_up(self):
        # One file moves cleanly, one cannot have its source removed.  The
        # move total, the breakdown, and the degraded line must agree.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "P8080001.ORF", b"stuck", when)
            write_file(source / "P8080002.ORF", b"clean", when)

            real_unlink = stackcopy.os.unlink
            real_rename = stackcopy.atomic_rename_no_replace
            seen: set[str] = set()

            def rename(src, dst):
                key = os.fspath(dst)
                if key not in seen and not os.path.basename(key).startswith(
                    stackcopy._TEMP_PREFIX
                ):
                    seen.add(key)
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return real_rename(src, dst)

            def unlink(path, **kwargs):
                if os.fspath(path).endswith("P8080001.ORF"):
                    raise PermissionError(errno.EACCES, "read-only card")
                return real_unlink(path, **kwargs)

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "atomic_rename_no_replace", side_effect=rename
                    ),
                    mock.patch.object(stackcopy.os, "unlink", side_effect=unlink),
                ),
            )

            self.assertEqual(code, 1, output)
            self.assertIn("Done. Imported 1 files", output)
            self.assertIn("1 remaining", output)
            self.assertIn("Imported normally: 1", output)
            self.assertIn(
                "Copied successfully but source could not be removed: 1", output
            )
            self.assertIn("Failures: 0", output)
            # Both files reached the destination; only the clean one left the card.
            self.assertEqual(
                set(files_under(lightroom)),
                {
                    Path("2026") / "2026-08-24" / "P8080001.ORF",
                    Path("2026") / "2026-08-24" / "P8080002.ORF",
                },
            )
            self.assertEqual(set(files_under(source)), {Path("P8080001.ORF")})

    def test_legacy_lightroom_mode_also_reports_it(self):
        # The tally lives where the outcome is decided, so every mode reports
        # it - not just --lightroomimport.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base + timedelta(seconds=number))
            write_file(
                source / "P8080004.JPG", b"stack output", base + timedelta(seconds=5)
            )

            code, output = run_main(
                ["--lightroom", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=self.cross_device_unlink_failure(source),
            )

            self.assertEqual(code, 1, output)
            self.assertIn("Sources still in place after a successful copy:", output)
            self.assertIn("were not removed:", output)
            self.assertIn("Re-running is safe", output)

    def test_no_recovery_duplicate_is_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.build(Path(tmp))
            base = datetime(2026, 8, 24, 12)
            # Make it a stack input so the recovery path would apply if the
            # result were mistaken for a failure.
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base + timedelta(seconds=number))
            write_file(
                source / "P8080004.JPG", b"stack output", base + timedelta(seconds=5)
            )

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={
                    "P8080004.JPG": stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                    )
                },
                extra_contexts=self.cross_device_unlink_failure(source),
            )

            self.assertEqual(code, 1, output)
            # Nothing was treated as a failed placement, so recovery never ran.
            self.assertNotIn("Import completed with recovery.", output)
            self.assertNotIn("recovered", output)
            self.assertIn("Copied successfully but source could not be removed", output)
            # Each file landed exactly once at its *planned* destination, with
            # no second copy anywhere in the Lightroom hierarchy.
            self.assertEqual(
                set(files_under(stack_input)),
                {
                    Path("2026") / "2026-08-24" / f"P808{number:04d}.{extension}"
                    for number in (1, 2, 3)
                    for extension in ("JPG", "ORF")
                },
            )
            self.assertEqual(
                set(files_under(lightroom)),
                {
                    Path("2026") / "2026-08-24" / "P8080004 stacked.JPG",
                    Path("2026") / "2026-08-24" / "P8081868.ORF",
                },
            )
            # And every original is still on the card, exactly once.
            self.assertEqual(len(files_under(source)), 8)

    def test_second_run_recognizes_the_destination_and_stays_consistent(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.build(Path(tmp))
            first_code, _first = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=self.cross_device_unlink_failure(source),
            )
            self.assertEqual(first_code, 1)

            # The card is untouched by the failure, so a rerun sees the
            # matching destination and finishes the move rather than
            # duplicating anything.
            second_code, second = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(second_code, 0, second)
            self.assertIn("already exists with identical content", second)
            self.assertEqual(files_under(source), {})
            self.assertEqual(
                files_under(lightroom / "2026" / "2026-08-24"),
                {Path("P8081868.ORF"): b"irreplaceable"},
            )

    def test_leave_on_card_copies_are_ordinary_successes(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, lightroom, stack_input = self.build(Path(tmp))

            code, output = run_main(
                ["--lightroomimport", str(source), "--leave-on-card"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("source could not be removed", output)
            self.assertIn("Sources left in place.", output)
            self.assertEqual(
                files_under(source), {Path("P8081868.ORF"): b"irreplaceable"}
            )


# ---------------------------------------------------------------------------
# Finding 8 - source inside a destination tree
# ---------------------------------------------------------------------------


class SourceInsideDestinationTests(unittest.TestCase):
    def attempt(self, source: Path, lightroom: Path, stack_input: Path):
        return run_main(
            ["--lightroomimport", str(source)],
            lightroom=lightroom,
            stack_input=stack_input,
            metadata={},
        )

    def test_source_equal_to_the_lightroom_root_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            lightroom.mkdir(parents=True)
            write_file(lightroom / "P8081868.ORF", b"filed", datetime(2026, 8, 24, 12))

            code, output = self.attempt(lightroom, lightroom, stack_input)

            self.assertEqual(code, 1, output)
            self.assertIn(
                "the source folder is the Lightroom import destination", output
            )
            # Nothing was scanned, planned, or moved.
            self.assertNotIn("Planned Lightroom import", output)
            self.assertEqual(files_under(lightroom), {Path("P8081868.ORF"): b"filed"})

    def test_source_nested_under_the_lightroom_root_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            nested = lightroom / "2026" / "2026-08-24"
            write_file(nested / "P8081868.ORF", b"filed", datetime(2026, 8, 24, 12))

            code, output = self.attempt(nested, lightroom, stack_input)

            self.assertEqual(code, 1, output)
            self.assertIn("is inside the Lightroom import destination", output)
            self.assertEqual(files_under(nested), {Path("P8081868.ORF"): b"filed"})

    def test_source_nested_under_the_stack_input_root_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            nested = stack_input / "2026" / "2026-08-24"
            write_file(nested / "P8081868.ORF", b"filed", datetime(2026, 8, 24, 12))

            code, output = self.attempt(nested, lightroom, stack_input)

            self.assertEqual(code, 1, output)
            self.assertIn("is inside the stack input destination", output)
            self.assertEqual(files_under(nested), {Path("P8081868.ORF"): b"filed"})

    def test_symlinked_source_cannot_defeat_the_check(self):
        if not hasattr(os, "symlink"):
            self.skipTest("platform has no symlinks")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            nested = lightroom / "2026" / "2026-08-24"
            write_file(nested / "P8081868.ORF", b"filed", datetime(2026, 8, 24, 12))
            link = root / "shortcut"
            try:
                os.symlink(nested, link, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("cannot create symlinks here")

            code, output = self.attempt(link, lightroom, stack_input)

            self.assertEqual(code, 1, output)
            self.assertIn("Lightroom import destination", output)

    def test_unrelated_source_still_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "P8081868.ORF", b"new", datetime(2026, 8, 24, 12))

            code, output = self.attempt(source, lightroom, stack_input)

            self.assertEqual(code, 0, output)
            self.assertEqual(files_under(source), {})

    def test_destination_under_the_source_still_works(self):
        # The inverse layout stays supported: the recursive scan excludes the
        # destination trees instead of refusing the run.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "Pictures"
            lightroom = source / "Lightroom"
            stack_input = source / "olympus.stack.input.photos"
            write_file(source / "P8081868.ORF", b"new", datetime(2026, 8, 24, 12))
            write_file(
                lightroom / "2026" / "2020-01-01" / "old.JPG",
                b"already filed",
                datetime(2020, 1, 1, 9),
            )

            code, output = self.attempt(source, lightroom, stack_input)

            self.assertEqual(code, 0, output)
            self.assertEqual(names_under(source) - names_under(lightroom), set())
            # The already-filed photo was not re-imported.
            self.assertIn(
                Path("2026") / "2020-01-01" / "old.JPG", set(files_under(lightroom))
            )

    def test_gui_validation_matches_the_cli(self):
        if stackcopy_gui is None:
            self.skipTest("customtkinter is not installed")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            nested = lightroom / "2026" / "2026-08-24"
            nested.mkdir(parents=True)
            card = root / "card"
            card.mkdir()

            self.assertIsNone(
                stackcopy_gui.source_inside_destination_error(
                    str(card), str(lightroom), str(stack_input)
                )
            )
            message = stackcopy_gui.source_inside_destination_error(
                str(nested), str(lightroom), str(stack_input)
            )
            assert message is not None
            self.assertIn("Lightroom destination", message)
            equal = stackcopy_gui.source_inside_destination_error(
                str(stack_input), str(lightroom), str(stack_input)
            )
            assert equal is not None
            self.assertIn("stack-input folder", equal)


# ---------------------------------------------------------------------------
# Finding 9 - st_ino == 0
# ---------------------------------------------------------------------------


class ZeroInodeIdentityTests(unittest.TestCase):
    def fingerprint(self, *, inode, size, device=1, mtime_ns=0):
        return stackcopy.FileFingerprint(
            device=device, inode=inode, size=size, mtime_ns=mtime_ns
        )

    def test_inode_identity_is_rejected_when_either_side_is_zero(self):
        real = self.fingerprint(inode=42, size=10)
        zero = self.fingerprint(inode=0, size=10)
        self.assertTrue(stackcopy.inode_identity_is_meaningful(real, real))
        self.assertFalse(stackcopy.inode_identity_is_meaningful(zero, real))
        self.assertFalse(stackcopy.inode_identity_is_meaningful(real, zero))
        self.assertFalse(stackcopy.inode_identity_is_meaningful(None, real))

    def test_two_different_files_reporting_inode_zero_are_a_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "a.ORF"
            dest = root / "b.ORF"
            write_file(src, b"source bytes", datetime(2026, 8, 24, 12))
            write_file(dest, b"other bytes!", datetime(2020, 1, 1, 9))

            def zero_inode(path):
                return self.fingerprint(inode=0, size=os.path.getsize(path), device=99)

            with mock.patch.object(
                stackcopy, "_file_fingerprint", side_effect=zero_inode
            ):
                check = stackcopy.classify_destination(str(src), str(dest))

            # Without the guard this would have been called IDENTICAL.
            self.assertEqual(check.state, stackcopy.DestinationState.CONFLICT)

    def test_identical_content_with_inode_zero_falls_through_to_content_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "a.ORF"
            dest = root / "b.ORF"
            write_file(src, b"same bytes", datetime(2026, 8, 24, 12))
            write_file(dest, b"same bytes", datetime(2020, 1, 1, 9))

            def zero_inode(path):
                return self.fingerprint(inode=0, size=os.path.getsize(path), device=99)

            with mock.patch.object(
                stackcopy, "_file_fingerprint", side_effect=zero_inode
            ):
                check = stackcopy.classify_destination(str(src), str(dest))

            self.assertEqual(check.state, stackcopy.DestinationState.IDENTICAL)

    def test_a_move_onto_itself_is_still_a_noop_with_inode_zero(self):
        # The dangerous case: if inode identity is unusable and the content
        # check says "identical", a move must still not delete the one file.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "P8081868.ORF"
            write_file(path, b"the only copy", datetime(2026, 8, 24, 12))

            def zero_inode(target):
                return self.fingerprint(
                    inode=0, size=os.path.getsize(target), device=99
                )

            output = io.StringIO()
            with (
                mock.patch.object(
                    stackcopy, "_file_fingerprint", side_effect=zero_inode
                ),
                redirect_stdout(output),
            ):
                result, moved = stackcopy.safe_file_operation(
                    "move", str(path), str(path), "moving", force=True
                )

            self.assertTrue(bool(result))
            self.assertEqual(moved, 0)
            self.assertTrue(path.exists())
            self.assertEqual(path.read_bytes(), b"the only copy")

    def test_meaningful_inodes_still_take_the_shortcut(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "a.ORF"
            link = root / "b.ORF"
            write_file(src, b"same file", datetime(2026, 8, 24, 12))
            try:
                os.link(src, link)
            except (OSError, AttributeError, NotImplementedError):
                self.skipTest("hard links unavailable here")

            check = stackcopy.classify_destination(str(src), str(link))
            self.assertEqual(check.state, stackcopy.DestinationState.IDENTICAL)
            self.assertTrue(stackcopy.is_same_physical_file(str(src), str(link), check))


# ---------------------------------------------------------------------------
# Finding 10 - companion files share one date folder
# ---------------------------------------------------------------------------


class CompanionDateConsistencyTests(unittest.TestCase):
    def test_jpg_raw_and_ori_straddling_midnight_stay_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            # The RAW is authoritative and lands on the 24th; the JPG and ORI
            # mtimes fall a fraction of a second later, on the 25th.
            write_file(
                source / "P8081234.ORF", b"raw", datetime(2026, 8, 24, 23, 59, 59)
            )
            write_file(source / "P8081234.JPG", b"jpg", datetime(2026, 8, 25, 0, 0, 0))
            write_file(source / "P8081234.ORI", b"ori", datetime(2026, 8, 25, 0, 0, 0))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(
                set(files_under(lightroom)),
                {
                    Path("2026") / "2026-08-24" / name
                    for name in ("P8081234.ORF", "P8081234.JPG", "P8081234.ORI")
                },
            )

    def test_a_video_keeps_its_own_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(
                source / "P8081234.ORF", b"raw", datetime(2026, 8, 24, 23, 59, 59)
            )
            write_file(source / "P8081234.JPG", b"jpg", datetime(2026, 8, 25, 0, 0, 0))
            write_file(source / "P8081234.MOV", b"mov", datetime(2026, 8, 25, 0, 0, 0))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            landed = set(files_under(lightroom))
            self.assertIn(Path("2026") / "2026-08-24" / "P8081234.ORF", landed)
            self.assertIn(Path("2026") / "2026-08-24" / "P8081234.JPG", landed)
            self.assertIn(Path("2026") / "2026-08-25" / "P8081234.MOV", landed)

    def test_ori_is_authoritative_when_there_is_no_raw(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(
                source / "P8081234.ORI", b"ori", datetime(2026, 8, 24, 23, 59, 59)
            )
            write_file(source / "P8081234.JPG", b"jpg", datetime(2026, 8, 25, 0, 0, 0))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(
                set(files_under(lightroom)),
                {
                    Path("2026") / "2026-08-24" / name
                    for name in ("P8081234.ORI", "P8081234.JPG")
                },
            )

    def test_date_filter_follows_the_canonical_companion_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(
                source / "P8081234.ORF", b"raw", datetime(2026, 8, 24, 23, 59, 59)
            )
            write_file(source / "P8081234.JPG", b"jpg", datetime(2026, 8, 25, 0, 0, 0))

            code, output = run_main(
                ["--lightroomimport", str(source), "--date", "2026-08-24"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            # The pair is selected or skipped together, never split.
            self.assertEqual(
                set(files_under(lightroom)),
                {
                    Path("2026") / "2026-08-24" / name
                    for name in ("P8081234.ORF", "P8081234.JPG")
                },
            )
            self.assertEqual(files_under(source), {})


# ---------------------------------------------------------------------------
# Finding 11 - --prefix validation
# ---------------------------------------------------------------------------


class PrefixValidationTests(unittest.TestCase):
    def test_ordinary_prefixes_are_accepted(self):
        for prefix in (None, "Forest", "Forest Trip", "Muir Woods 2026", "été"):
            with self.subTest(prefix=prefix):
                self.assertIsNone(stackcopy.prefix_validation_error(prefix))

    def test_path_separators_are_rejected(self):
        for prefix in ("a/b", "../escape", "a\\b", "/", "\\"):
            with self.subTest(prefix=prefix):
                self.assertEqual(
                    stackcopy.prefix_validation_error(prefix),
                    "--prefix may not contain path separators.",
                )

    def assert_rejected(self, prefix: str, fragment: str) -> None:
        error = stackcopy.prefix_validation_error(prefix)
        assert error is not None, f"{prefix!r} should have been rejected"
        self.assertIn(fragment, error)

    def test_nul_and_control_characters_are_rejected(self):
        self.assert_rejected("a\0b", "NUL")
        self.assert_rejected("a\tb", "control")
        self.assert_rejected("a\nb", "control")

    def test_dot_prefixes_are_rejected(self):
        for prefix in (".", "..", " .. "):
            with self.subTest(prefix=prefix):
                self.assert_rejected(prefix, "'.' or '..'")

    def test_windows_reserved_characters_are_rejected_on_windows_only(self):
        with mock.patch.object(stackcopy, "IS_WINDOWS", False):
            self.assertIsNone(stackcopy.prefix_validation_error("9:30 am"))
        with mock.patch.object(stackcopy, "IS_WINDOWS", True):
            self.assert_rejected("9:30 am", "on Windows")

    def test_import_refuses_an_invalid_prefix_before_touching_anything(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "P8081868.JPG", b"photo", datetime(2026, 8, 24, 12))
            before = files_under(source)

            code, output = run_main(
                ["--lightroomimport", str(source), "--prefix", "trips/2026"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 1, output)
            self.assertEqual(
                output.strip(), "Error: --prefix may not contain path separators."
            )
            self.assertEqual(files_under(source), before)
            self.assertEqual(files_under(lightroom), {})

    def test_a_legitimate_prefix_still_renames_the_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base + timedelta(seconds=number))
            write_file(
                source / "P8080004.JPG", b"stack output", base + timedelta(seconds=5)
            )

            code, output = run_main(
                ["--lightroomimport", str(source), "--prefix", "Muir Woods"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={
                    "P8080004.JPG": stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                    )
                },
            )

            self.assertEqual(code, 0, output)
            self.assertEqual(
                names_under(lightroom), {"P8080004 Muir Woods stacked.JPG"}
            )


if __name__ == "__main__":
    unittest.main()
