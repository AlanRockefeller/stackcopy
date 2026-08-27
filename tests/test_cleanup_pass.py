"""Regression coverage for the 1.6.0 cleanup pass.

Six reviewed findings, one class each:

1. case-insensitive path safety on Windows-backed / WSL paths;
2. fsync ordering for read-only, protected source files;
3. the ambiguous-stem summary counting every physical file left behind;
4. mutually exclusive placement / recovery / source-remains accounting;
5. legacy --lightroom source-remains accounting;
6. the adjacent-folder rollover rule, which is asserted to stay conservative.

Nothing here needs ExifTool, a real read-only card, root access, WSL, or a
particular filesystem: the WSL semantics are simulated portably.
"""

import errno
import os
import stat
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402

from test_final_reliability_pass import (  # noqa: E402
    files_under,
    make_pair,
    run_main,
    write_file,
)


def _load_gui_module():
    """Import stackcopy_gui, stubbing its Tk dependencies when they are absent.

    The GUI's path validation is pure logic and must be tested even on a CLI-
    only machine, so customtkinter/tkinter are stood in for rather than skipped
    over.  Only the import-time names the module touches are provided.
    """
    try:
        import stackcopy_gui

        return stackcopy_gui
    except ImportError:
        pass

    import types

    class _Any(types.ModuleType):
        """A module whose every attribute is a usable placeholder class."""

        def __getattr__(self, name):
            if name.startswith("__"):
                raise AttributeError(name)
            placeholder = type(name, (object,), {})
            setattr(self, name, placeholder)
            return placeholder

    stubs = {}
    for name in (
        "customtkinter",
        "tkinter",
        "tkinter.filedialog",
        "tkinter.messagebox",
    ):
        if name not in sys.modules:
            stubs[name] = _Any(name)
    with mock.patch.dict(sys.modules, stubs):
        try:
            import stackcopy_gui

            return stackcopy_gui
        except Exception:  # pragma: no cover - depends on the local environment
            return None


stackcopy_gui = _load_gui_module()


# ---------------------------------------------------------------------------
# Finding 1 - normcase() alone is not case folding under WSL
# ---------------------------------------------------------------------------


class WindowsBackedPathSafetyTests(unittest.TestCase):
    """/mnt/c/Photos and /mnt/c/photos are one place, on Linux normcase or not."""

    def test_mnt_drive_paths_are_treated_as_case_insensitive(self):
        self.assertTrue(
            stackcopy.path_comparison_is_case_insensitive("/mnt/c/Photos/P8081234.JPG")
        )
        self.assertTrue(stackcopy.path_comparison_is_case_insensitive("/mnt/e"))

    def test_ordinary_posix_paths_stay_case_sensitive(self):
        if stackcopy.IS_WINDOWS or stackcopy.IS_MACOS:
            self.skipTest("this platform is case-insensitive everywhere")
        for path in ("/home/alan/Pictures", "/tmp/Card", "/var/tmp/A.JPG", "/mnt"):
            with self.subTest(path=path):
                self.assertFalse(stackcopy.path_comparison_is_case_insensitive(path))

    def test_same_file_detection_folds_case_on_windows_backed_paths(self):
        # The critical scenario: identical content, inode identity unavailable,
        # and only the spelling differs.  Concluding "not the same file" here
        # would let a move unlink the only copy.
        self.assertTrue(
            stackcopy.paths_resolve_to_same_file(
                "/mnt/c/Photos/P8081234.JPG", "/mnt/c/photos/p8081234.jpg"
            )
        )

    def test_same_file_detection_keeps_posix_paths_distinct(self):
        if stackcopy.IS_WINDOWS or stackcopy.IS_MACOS:
            self.skipTest("this platform is case-insensitive everywhere")
        self.assertFalse(
            stackcopy.paths_resolve_to_same_file(
                "/home/alan/Photos/P8081234.JPG", "/home/alan/photos/p8081234.jpg"
            )
        )
        # And genuinely unrelated Windows-backed paths still differ.
        self.assertFalse(
            stackcopy.paths_resolve_to_same_file(
                "/mnt/c/Photos/P8081234.JPG", "/mnt/c/Photos/P8081235.JPG"
            )
        )

    def test_containment_is_not_bypassable_with_case_only_differences(self):
        self.assertTrue(
            stackcopy.path_is_within("/mnt/c/users/alan/pictures", "/mnt/c/Users/Alan")
        )
        self.assertTrue(
            stackcopy.path_is_within("/mnt/c/Users/Alan", "/mnt/c/users/alan")
        )
        self.assertFalse(
            stackcopy.path_is_within("/mnt/c/Users/Alan", "/mnt/c/Users/Bob")
        )
        # A sibling whose name merely starts with the root is not inside it.
        self.assertFalse(
            stackcopy.path_is_within("/mnt/c/Pictures2", "/mnt/c/pictures")
        )

    def test_lightroom_import_source_conflict_inherits_the_folding(self):
        message = stackcopy.lightroom_import_source_conflict(
            "/mnt/c/photos/lightroom/2026",
            "/mnt/c/Photos/Lightroom",
            "/mnt/c/Photos/StackInput",
        )
        self.assertIsNotNone(message)
        self.assertIn("is inside", message)
        self.assertIn("Lightroom import destination", message)

        equal = stackcopy.lightroom_import_source_conflict(
            "/mnt/c/photos/stackinput",
            "/mnt/c/Photos/Lightroom",
            "/mnt/c/Photos/StackInput",
        )
        self.assertIsNotNone(equal)
        self.assertIn("stack input destination", equal)

        self.assertIsNone(
            stackcopy.lightroom_import_source_conflict(
                "/mnt/c/DCIM/100OLYMP",
                "/mnt/c/Photos/Lightroom",
                "/mnt/c/Photos/StackInput",
            )
        )

    @unittest.skipIf(stackcopy_gui is None, "customtkinter is not installed")
    def test_gui_matches_the_cli_on_the_same_layouts(self):
        layouts = (
            ("/mnt/c/photos/lightroom/2026", "/mnt/c/Photos/Lightroom", "/mnt/c/S"),
            ("/mnt/c/PHOTOS/S", "/mnt/c/Photos/Lightroom", "/mnt/c/photos/s"),
            ("/mnt/c/DCIM/100OLYMP", "/mnt/c/Photos/Lightroom", "/mnt/c/S"),
            ("/mnt/c/Pictures2", "/mnt/c/pictures", "/mnt/c/S"),
        )
        for source, lightroom, stack_input in layouts:
            with self.subTest(source=source):
                cli = stackcopy.lightroom_import_source_conflict(
                    source, lightroom, stack_input
                )
                gui = stackcopy_gui.source_inside_destination_error(
                    source, lightroom, stack_input
                )
                self.assertEqual(cli is None, gui is None)
                if cli is not None:
                    relation = "is inside" if "is inside" in cli else "is"
                    self.assertIn(relation, gui)

    @unittest.skipIf(stackcopy_gui is None, "customtkinter is not installed")
    def test_gui_shares_the_cli_helper(self):
        self.assertIs(stackcopy_gui.path_is_within, stackcopy.path_is_within)


class InodeZeroSameFileMoveTests(unittest.TestCase):
    """A move must never unlink a source that *is* the destination."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def inode_zero(self):
        """Report every file as inode 0, as some FUSE/9P mounts really do."""
        real = stackcopy._file_fingerprint

        def fingerprint(path):
            result = real(path)
            if result is None:
                return None
            return stackcopy.FileFingerprint(
                device=result.device,
                inode=0,
                size=result.size,
                mtime_ns=result.mtime_ns,
            )

        return mock.patch.object(
            stackcopy, "_file_fingerprint", side_effect=fingerprint
        )

    def test_case_different_same_file_move_is_a_no_op(self):
        # Two real files stand in for the one file a case-insensitive volume
        # would show under both spellings; the case-insensitivity of the
        # volume is simulated so the test runs anywhere.
        when = datetime(2026, 8, 24, 12)
        src = self.root / "Photos" / "P8081234.JPG"
        dest = self.root / "photos" / "p8081234.jpg"
        write_file(src, b"the only copy", when)
        write_file(dest, b"the only copy", when)

        with (
            self.inode_zero(),
            mock.patch.object(
                stackcopy, "path_comparison_is_case_insensitive", return_value=True
            ),
        ):
            self.assertFalse(
                stackcopy.inode_identity_is_meaningful(
                    stackcopy._file_fingerprint(str(src)),
                    stackcopy._file_fingerprint(str(dest)),
                )
            )
            result, moved = stackcopy.safe_file_operation(
                "move", str(src), str(dest), "moving test file"
            )

        self.assertTrue(bool(result))
        self.assertEqual(moved, 0)
        # Neither spelling was unlinked.
        self.assertTrue(src.exists())
        self.assertTrue(dest.exists())
        self.assertEqual(src.read_bytes(), b"the only copy")

    def test_genuinely_distinct_files_still_move(self):
        # Same inode-zero mount, but the paths are not case variants: the move
        # must still happen, or the safety check would have become a stall.
        when = datetime(2026, 8, 24, 12)
        src = self.root / "card" / "P8081234.JPG"
        dest = self.root / "dest" / "P8081234.JPG"
        write_file(src, b"the only copy", when)
        dest.parent.mkdir(parents=True, exist_ok=True)

        with self.inode_zero():
            result, _moved = stackcopy.safe_file_operation(
                "move", str(src), str(dest), "moving test file"
            )

        self.assertTrue(bool(result))
        self.assertFalse(src.exists())
        self.assertEqual(dest.read_bytes(), b"the only copy")


# ---------------------------------------------------------------------------
# Finding 2 - copystat must not be able to break the durability flush
# ---------------------------------------------------------------------------


class ProtectedSourceDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.src = self.root / "card" / "P8081868.ORF"
        self.dest = self.root / "dest" / "P8081868.ORF"
        write_file(self.src, b"irreplaceable", datetime(2026, 8, 24, 12))
        self.dest.parent.mkdir(parents=True, exist_ok=True)

    def cross_device(self):
        """Force the durable copy path by reporting EXDEV for the fast rename.

        The fast path is the no-clobber commit primitive, not os.replace(), so
        that is where a cross-filesystem move announces itself.
        """
        real_rename = stackcopy.atomic_rename_no_replace
        state = {"first": True}

        def rename(src, dst):
            if state["first"] and os.fspath(dst) == str(self.dest):
                state["first"] = False
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            return real_rename(src, dst)

        return mock.patch.object(
            stackcopy, "atomic_rename_no_replace", side_effect=rename
        )

    def test_read_only_source_still_moves_durably(self):
        os.chmod(self.src, stat.S_IRUSR)
        self.addCleanup(
            lambda: self.src.exists()
            and os.chmod(self.src, stat.S_IRUSR | stat.S_IWUSR)
        )

        with self.cross_device():
            result, _moved = stackcopy.safe_file_operation(
                "move", str(self.src), str(self.dest), "moving test file"
            )

        self.assertTrue(bool(result))
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.SUCCESS
        )
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        # The destination inherited the source's restrictive mode bits...
        self.assertEqual(stat.S_IMODE(self.dest.stat().st_mode), stat.S_IRUSR)
        # ...and the source was removed, so the move really completed.
        self.assertFalse(self.src.exists())

    def test_metadata_is_applied_while_the_flush_descriptor_is_open(self):
        # A mode-independent ordering assertion, so this holds as root and on
        # filesystems that ignore permission bits.
        calls: list[str] = []
        real_copyfile = stackcopy.shutil.copyfile
        real_copystat = stackcopy.shutil.copystat
        real_os_open = stackcopy.os.open

        def copyfile(src, dst, **kwargs):
            calls.append("copyfile")
            return real_copyfile(src, dst, **kwargs)

        def os_open(path, flags, *args, **kwargs):
            if flags & os.O_RDWR:
                calls.append("open_temp_writable")
            return real_os_open(path, flags, *args, **kwargs)

        def copystat(src, dst, **kwargs):
            calls.append("copystat")
            return real_copystat(src, dst, **kwargs)

        def fsync_file(fd):
            calls.append("fsync_file")
            # The descriptor handed over is real, writable, and open.
            os.fstat(fd)
            return os.fsync(fd)

        with (
            self.cross_device(),
            mock.patch.object(stackcopy.shutil, "copyfile", side_effect=copyfile),
            mock.patch.object(stackcopy.os, "open", side_effect=os_open),
            mock.patch.object(stackcopy.shutil, "copystat", side_effect=copystat),
            mock.patch.object(stackcopy, "fsync_file", side_effect=fsync_file),
        ):
            result, _moved = stackcopy.safe_file_operation(
                "move", str(self.src), str(self.dest), "moving test file"
            )

        self.assertTrue(bool(result))
        self.assertEqual(
            calls[:4], ["copyfile", "open_temp_writable", "copystat", "fsync_file"]
        )
        self.assertFalse(self.src.exists())

    def test_a_failed_flush_on_a_protected_source_keeps_the_source(self):
        os.chmod(self.src, stat.S_IRUSR)
        self.addCleanup(
            lambda: self.src.exists()
            and os.chmod(self.src, stat.S_IRUSR | stat.S_IWUSR)
        )

        with (
            self.cross_device(),
            mock.patch.object(
                stackcopy,
                "fsync_file",
                side_effect=OSError(errno.EIO, "I/O error"),
            ),
        ):
            result, _moved = stackcopy.safe_file_operation(
                "move", str(self.src), str(self.dest), "moving test file"
            )

        self.assertFalse(bool(result))
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.FAILED
        )
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertFalse(self.dest.exists())
        # No temp file was left behind, even though it was made read-only.
        self.assertEqual(list(self.dest.parent.iterdir()), [])


class DirectoryFsyncErrnoClassificationTests(unittest.TestCase):
    """ "Denied" is not proof that directory fsync is unimplemented."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        stackcopy._directory_fsync_warning_shown = False

    def sync_with(self, error, *, at):
        real_open = stackcopy.os.open

        def os_open(path, flags, *args, **kwargs):
            if at == "open" and flags & getattr(os, "O_DIRECTORY", 0):
                raise error
            return real_open(path, flags, *args, **kwargs)

        def fsync(fd):
            if at == "fsync":
                raise error
            return None

        with (
            mock.patch.object(stackcopy.os, "open", side_effect=os_open),
            mock.patch.object(stackcopy.os, "fsync", side_effect=fsync),
        ):
            return stackcopy.fsync_directory(str(self.directory))

    @unittest.skipUnless(
        hasattr(os, "O_DIRECTORY"), "directory fsync is unsupported here"
    )
    def test_unimplemented_errnos_are_unsupported(self):
        for name in ("EINVAL", "ENOSYS", "ENOTSUP"):
            code = getattr(errno, name, None)
            if code is None:  # pragma: no cover - platform dependent
                continue
            with self.subTest(errno=name):
                self.assertIs(
                    self.sync_with(OSError(code, name), at="fsync"),
                    stackcopy.DirectorySync.UNSUPPORTED,
                )

    @unittest.skipUnless(
        hasattr(os, "O_DIRECTORY"), "directory fsync is unsupported here"
    )
    def test_eacces_is_a_failure_not_an_unsupported_operation(self):
        for at in ("open", "fsync"):
            with self.subTest(at=at):
                self.assertIs(
                    self.sync_with(OSError(errno.EACCES, "denied"), at=at),
                    stackcopy.DirectorySync.FAILED,
                )

    @unittest.skipUnless(
        hasattr(os, "O_DIRECTORY"), "directory fsync is unsupported here"
    )
    def test_eperm_is_a_failure_at_either_step(self):
        # EINVAL is POSIX's "this object cannot be fsynced"; EPERM is not.  An
        # unexplained denial is ambiguous, and ambiguity keeps the source.
        for at in ("open", "fsync"):
            with self.subTest(at=at):
                self.assertIs(
                    self.sync_with(OSError(errno.EPERM, "denied"), at=at),
                    stackcopy.DirectorySync.FAILED,
                )


# ---------------------------------------------------------------------------
# Finding 3 - an ambiguous stem leaves its whole stem behind
# ---------------------------------------------------------------------------


class AmbiguousStemFileCountTests(unittest.TestCase):
    def run_import(self, files):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            when = datetime(2026, 8, 24, 12)
            for name in files:
                write_file(source / name, name.encode(), when)
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=root / "Lightroom",
                stack_input=root / "StackInput",
                metadata={},
            )
            return code, output, files_under(source)

    def test_two_raws_only(self):
        code, output, left = self.run_import(["P8081868.ORF", "P8081868.DNG"])
        self.assertEqual(code, 1, output)
        self.assertIn("Ambiguous stems left untouched: 1 (2 files)", output)
        self.assertEqual(len(left), 2)

    def test_jpg_plus_two_raws_counts_the_jpg_too(self):
        code, output, left = self.run_import(
            ["P8081868.JPG", "P8081868.ORF", "P8081868.DNG"]
        )
        self.assertEqual(code, 1, output)
        self.assertIn("Ambiguous stems left untouched: 1 (3 files)", output)
        self.assertEqual(len(left), 3)

    def test_ori_companion_counts_as_well(self):
        code, output, left = self.run_import(
            ["P8081868.JPG", "P8081868.ORF", "P8081868.DNG", "P8081868.ORI"]
        )
        self.assertEqual(code, 1, output)
        self.assertIn("Ambiguous stems left untouched: 1 (4 files)", output)
        self.assertEqual(len(left), 4)

    def test_multiple_ambiguous_stems_add_up(self):
        files = [
            "P8081868.JPG",
            "P8081868.ORF",
            "P8081868.DNG",
            "P8081869.ORF",
            "P8081869.DNG",
        ]
        code, output, left = self.run_import(files)
        self.assertEqual(code, 1, output)
        self.assertIn("Ambiguous stems left untouched: 2 (5 files)", output)
        # The reported count is exactly what is still on the card.
        self.assertEqual(len(left), len(files))


# ---------------------------------------------------------------------------
# Finding 4 - normal, recovered and source-remains are mutually exclusive
# ---------------------------------------------------------------------------


class ImportOutcomeAccountingTests(unittest.TestCase):
    def test_one_run_with_all_three_outcomes(self):
        # One run that produces every mutually exclusive outcome at once:
        #   P8080001 / P8080003  - stack inputs placed normally
        #   P8080004.JPG         - the stack output, placed normally
        #   P8080002             - every planned stack placement fails, so the
        #                          stem is recovered into the Lightroom tree
        #   P8080009.ORF         - a remaining file that is copied across a
        #                          device boundary but whose source is stuck
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2, 3):
                make_pair(source, f"P808{number:04d}", base.replace(second=number))
            write_file(source / "P8080004.JPG", b"stack output", base.replace(second=5))
            write_file(source / "P8080009.ORF", b"stuck", base.replace(second=30))

            real_operation = stackcopy.safe_file_operation
            real_rename = stackcopy.atomic_rename_no_replace
            real_unlink = os.unlink

            def flaky(operation, src, dest, description, *rest):
                if Path(src).stem == "P8080002" and "recovered" not in description:
                    return False, 0
                return real_operation(operation, src, dest, description, *rest)

            def rename(src, dst):
                # Force the durable cross-device copy path for the stuck file.
                if os.fspath(src).endswith("P8080009.ORF"):
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return real_rename(src, dst)

            def unlink(path, **kwargs):
                parts = Path(os.fspath(path)).parts
                if parts[-2:] == ("card", "P8080009.ORF"):
                    raise PermissionError(errno.EACCES, "read-only card")
                return real_unlink(path, **kwargs)

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
                    mock.patch.object(
                        stackcopy, "atomic_rename_no_replace", side_effect=rename
                    ),
                    mock.patch.object(stackcopy.os, "unlink", side_effect=unlink),
                ),
            )

            self.assertEqual(code, 1, output)
            # 5 normal + 2 recovered + 1 stuck source = every one of the 8 files.
            self.assertIn("Files safely placed: 8", output)
            self.assertIn("Imported normally: 5", output)
            self.assertIn("Recovered to fallback destination: 2", output)
            self.assertIn(
                "Copied successfully but source could not be removed: 1", output
            )
            self.assertIn("Failures: 0", output)
            # A recovered file is never folded into the normal-import count,
            # and neither is one whose source is still on the card.
            self.assertNotIn("Imported normally: 7", output)
            self.assertNotIn("Imported normally: 8", output)
            self.assertIn("Import completed with recovery.", output)
            # The headline counts only the files that actually left the card.
            self.assertIn("Done. Imported 7 files", output)
            self.assertIn("2 recovered; 0 unrecovered", output)

            # Every file reached a destination exactly once.
            placed = sorted(
                [path.name for path in files_under(lightroom)]
                + [path.name for path in files_under(stack_input)]
            )
            self.assertEqual(
                placed,
                sorted(
                    [f"P808000{n}.{ext}" for n in (1, 2, 3) for ext in ("JPG", "ORF")]
                    + ["P8080004 stacked.JPG", "P8080009.ORF"]
                ),
            )
            # The recovered stem went to the fallback tree, not the planned one.
            self.assertNotIn(
                "P8080002.ORF", {path.name for path in files_under(stack_input)}
            )
            # Only the file whose source could not be removed is still there.
            self.assertEqual(
                sorted(path.name for path in files_under(source)), ["P8080009.ORF"]
            )

    def test_failures_line_stays_the_unrecovered_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "P8080001.ORF", b"raw", datetime(2026, 8, 24, 12))

            def rename(src, dst):
                raise OSError(errno.EIO, "every destination is broken")

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "atomic_rename_no_replace", side_effect=rename
                    ),
                    mock.patch.object(
                        stackcopy.shutil,
                        "copyfile",
                        side_effect=OSError(errno.EIO, "broken"),
                    ),
                ),
            )

            self.assertEqual(code, 1, output)
            self.assertIn("Files safely placed: 0", output)
            self.assertIn("Imported normally: 0", output)
            self.assertIn("Recovered to fallback destination: 0", output)
            self.assertIn("Failures: 1", output)
            self.assertEqual(
                sorted(path.name for path in files_under(source)), ["P8080001.ORF"]
            )


# ---------------------------------------------------------------------------
# Finding 5 - legacy --lightroom must not call a stuck source "moved"
# ---------------------------------------------------------------------------


class LegacyLightroomSourceRemainsTests(unittest.TestCase):
    def test_a_stuck_source_is_not_counted_as_moved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            # Four bracketed inputs plus an unpaired JPG output above them.
            for index in range(4):
                make_pair(
                    source,
                    f"P808000{index + 1}",
                    base.replace(second=index * 2),
                )
            write_file(source / "P8080005.JPG", b"stack output", base.replace(minute=1))

            real_rename = stackcopy.atomic_rename_no_replace
            real_unlink = os.unlink

            def rename(src, dst):
                # Force the cross-device copy path for one input file only.
                if os.fspath(src).endswith("P8080001.ORF"):
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                return real_rename(src, dst)

            def unlink(path, **kwargs):
                if os.fspath(path).endswith("P8080001.ORF"):
                    raise PermissionError(errno.EACCES, "read-only card")
                return real_unlink(path, **kwargs)

            code, output = run_main(
                ["--lightroom", str(source), "--jobs", "1"],
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

            # Degraded, not clean.
            self.assertEqual(code, 1, output)
            # Eight input files were planned; only seven actually moved.
            self.assertIn("Moved 7 input files", output)
            self.assertNotIn("Moved 8 input files", output)
            self.assertIn(
                "1 input file(s) reached the destination but their sources "
                "could not be removed",
                output,
            )
            self.assertIn("Sources still in place after a successful copy: 1", output)
            # The destination holds the file, and the card still holds it too.
            self.assertEqual(
                sorted(path.name for path in files_under(stack_input)),
                sorted(
                    [
                        f"P808000{i + 1}.{ext}"
                        for i in range(4)
                        for ext in ("JPG", "ORF")
                    ]
                ),
            )
            # --lightroom renames the stack output in place, so it stays put.
            self.assertEqual(
                sorted(path.name for path in files_under(source)),
                ["P8080001.ORF", "P8080005 stacked.JPG"],
            )


# ---------------------------------------------------------------------------
# Finding 6 - the adjacent-folder rule stays conservative
# ---------------------------------------------------------------------------


class AdjacentFolderRolloverTests(unittest.TestCase):
    """The reviewed rollover concern cannot manifest in this naming scheme."""

    def test_the_parsed_number_carries_the_date_component(self):
        # "P8081868" is P + month 8 + day 08 + frame 1868, and the whole digit
        # run becomes the number, so a same-day 9999 -> 0001 DCF rollover reads
        # as a decrease rather than an increment.
        parsed = {}
        for stem in ("P8089998", "P8089999", "P8080001", "P8080002"):
            match = stackcopy.NUMERIC_STEM_REGEX.fullmatch(stem)
            self.assertIsNotNone(match)
            parsed[stem] = (match.group(1), int(match.group(2)))

        self.assertEqual(parsed["P8089999"][0], parsed["P8080001"][0])
        self.assertGreater(parsed["P8089999"][1], parsed["P8080001"][1])

    def test_a_rollover_boundary_is_never_numerically_adjacent(self):
        # Even if both folders' sequences were merged, scan_stack_inputs walks
        # strictly by number - 1, so 8080001 can never reach 8089999.  Nothing
        # is gained by loosening the range rule, and the safety invariant that
        # the previous folder may only extend past the boundary is kept.
        rollover_pairs = (
            ("P8089999", "P8080001"),  # same-day rollover
            ("P8089999", "P8090001"),  # rollover across midnight
        )
        for previous, current in rollover_pairs:
            with self.subTest(previous=previous, current=current):
                previous_num = int(
                    stackcopy.NUMERIC_STEM_REGEX.fullmatch(previous).group(2)
                )
                current_num = int(
                    stackcopy.NUMERIC_STEM_REGEX.fullmatch(current).group(2)
                )
                self.assertNotEqual(current_num - 1, previous_num)

    def test_a_previous_folder_still_extends_an_ordinary_boundary(self):
        # The supported case: 100OLYMP ends below where 101OLYMP begins, the
        # numbers are contiguous, and the stack spans the folder boundary.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card" / "DCIM"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for index, stem in enumerate(("P8081866", "P8081867")):
                make_pair(source / "100OLYMP", stem, base.replace(second=index * 2))
            make_pair(source / "101OLYMP", "P8081868", base.replace(second=4))
            write_file(
                source / "101OLYMP" / "P8081869.JPG",
                b"stack output",
                base.replace(second=6),
            )

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            # All six input files crossed into the stack-input tree.
            self.assertEqual(len(files_under(stack_input)), 6)

    def test_an_overlapping_previous_folder_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card" / "DCIM"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            # The previous folder's numbering overlaps this folder's range, so
            # its P8081866 must not be adopted to fill the gap here.
            make_pair(source / "100OLYMP", "P8081866", base.replace(second=0))
            make_pair(source / "100OLYMP", "P8081870", base.replace(second=8))
            for index, stem in enumerate(("P8081867", "P8081868")):
                make_pair(source / "101OLYMP", stem, base.replace(second=2 + index * 2))
            write_file(
                source / "101OLYMP" / "P8081869.JPG",
                b"stack output",
                base.replace(second=6),
            )

            code, output = run_main(
                ["--lightroomimport", str(source), "--debug-stacks"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("overlaps this folder's numbering", output)
            # The previous folder's frames stayed out of the stack.
            self.assertNotIn(
                "P8081866", {path.name for path in files_under(stack_input)}
            )


if __name__ == "__main__":
    unittest.main()
