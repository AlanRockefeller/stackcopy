"""Regression coverage for the destination check-to-commit race.

Every scenario here forces the filesystem to change at the exact moment that
used to be dangerous, rather than hoping a real race shows up: the injection
sites wrap Stackcopy's own commit primitives, so the primitives themselves run
unmocked and it is genuinely the kernel that decides who wins.

The invariants under test are the same in all of them - a file Stackcopy did
not plan to touch is never destroyed, a source is never deleted unless its
content is provably safe somewhere else, and a run never reports a clean
success it could not guarantee.
"""

import errno
import io
import os
import stat
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402


def write_file(path: Path, content: bytes, when: datetime | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if when is not None:
        timestamp = when.timestamp()
        os.utime(path, (timestamp, timestamp))


def sidecars(directory: Path) -> list[str]:
    """Temp/guard/pin files Stackcopy should never leave behind."""
    return sorted(
        item.name
        for item in directory.iterdir()
        if item.name.startswith(
            (
                stackcopy._TEMP_PREFIX,
                stackcopy._GUARD_PREFIX,
                stackcopy._PIN_PREFIX,
            )
        )
    )


class OperationHarness(unittest.TestCase):
    """One source, one destination, and hooks to disturb the destination."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.src = self.root / "card" / "P8081868.ORF"
        self.dest_dir = self.root / "dest"
        self.dest = self.dest_dir / "P8081868.ORF"
        write_file(self.src, b"irreplaceable", datetime(2026, 8, 24, 12))
        self.dest_dir.mkdir(parents=True, exist_ok=True)

    def run_operation(self, operation="move", *, force=False, contexts=()):
        output = io.StringIO()
        with ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)
            with redirect_stdout(output):
                result, moved = stackcopy.safe_file_operation(
                    operation,
                    str(self.src),
                    str(self.dest),
                    "moving test file",
                    force=force,
                )
        return result, moved, output.getvalue()

    def intrude_at_commit(self, content: bytes):
        """Create a differing destination immediately before the commit.

        The real no-clobber primitive still runs, so this reproduces exactly
        the window the old code lost: classification said the name was free,
        and it no longer is by the time the rename happens.
        """
        real_rename = stackcopy.atomic_rename_no_replace
        fired = {"done": False}

        def rename(src, dst):
            if not fired["done"] and os.fspath(dst) == str(self.dest):
                fired["done"] = True
                write_file(self.dest, content)
            return real_rename(src, dst)

        return mock.patch.object(
            stackcopy, "atomic_rename_no_replace", side_effect=rename
        )

    def intrude_before_quarantine(self, content: bytes):
        """Change the destination after it was classified, before it is guarded."""
        real_quarantine = stackcopy.quarantine_destination

        def quarantine(dest_path):
            write_file(self.dest, content)
            return real_quarantine(dest_path)

        return mock.patch.object(
            stackcopy, "quarantine_destination", side_effect=quarantine
        )


# ---------------------------------------------------------------------------
# A - the destination was absent, and something else created it
# ---------------------------------------------------------------------------


class AbsentDestinationRaceTests(OperationHarness):
    def test_a_file_that_appears_before_the_commit_is_not_overwritten(self):
        result, _moved, output = self.run_operation(
            contexts=(self.intrude_at_commit(b"somebody else's photo"),)
        )

        self.assertFalse(bool(result))
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.FAILED
        )
        # The intruding file is intact and the source is still on the card.
        self.assertEqual(self.dest.read_bytes(), b"somebody else's photo")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertIn("appeared while Stackcopy was", output)
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_a_copy_that_loses_the_race_leaves_both_files_alone(self):
        result, _moved, output = self.run_operation(
            "copy", contexts=(self.intrude_at_commit(b"somebody else's photo"),)
        )

        self.assertFalse(bool(result))
        self.assertEqual(self.dest.read_bytes(), b"somebody else's photo")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertIn("appeared while Stackcopy was", output)
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_losing_the_race_is_not_reported_as_a_forced_overwrite(self):
        stackcopy.forced_overwrite_paths.clear()
        self.addCleanup(stackcopy.forced_overwrite_paths.clear)

        result, _moved, output = self.run_operation(
            force=True, contexts=(self.intrude_at_commit(b"somebody else's photo"),)
        )

        self.assertFalse(bool(result))
        self.assertFalse(stackcopy.forced_overwrite_of(result))
        self.assertEqual(stackcopy.forced_overwrite_paths, [])
        self.assertEqual(self.dest.read_bytes(), b"somebody else's photo")

    def test_a_normal_absent_destination_still_succeeds(self):
        result, moved, output = self.run_operation()

        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.SUCCESS
        )
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertFalse(self.src.exists())
        self.assertEqual(sidecars(self.dest_dir), [])
        self.assertEqual(moved, 0, output)

    def test_hard_link_move_keeps_and_reports_both_links_if_unlink_fails(self):
        unsupported = stackcopy.NoReplaceUnsupported(errno.ENOSYS, "no native rename")
        real_unlink = stackcopy.os.unlink
        stackcopy.source_remains_paths.clear()
        self.addCleanup(stackcopy.source_remains_paths.clear)

        def unlink(path, **kwargs):
            if os.fspath(path) == str(self.src):
                raise PermissionError(errno.EACCES, "read-only card")
            return real_unlink(path, **kwargs)

        result, moved, output = self.run_operation(
            contexts=(
                mock.patch.object(
                    stackcopy, "_native_rename_no_replace", side_effect=unsupported
                ),
                mock.patch.object(stackcopy.os, "unlink", side_effect=unlink),
            )
        )

        self.assertEqual(
            stackcopy.operation_outcome_of(result),
            stackcopy.OperationOutcome.COPIED_SOURCE_REMAINS,
        )
        self.assertEqual(moved, 0)
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertEqual(stackcopy.source_remains_paths, [str(self.src)])
        self.assertIn("did not unlink the destination", output)


# ---------------------------------------------------------------------------
# B - the destination was a zero-byte file Stackcopy meant to repair
# ---------------------------------------------------------------------------


class ZeroByteRecoveryRaceTests(OperationHarness):
    def setUp(self):
        super().setUp()
        write_file(self.dest, b"")

    def test_a_zero_byte_destination_that_fills_up_is_not_replaced(self):
        stackcopy.forced_overwrite_paths.clear()
        self.addCleanup(stackcopy.forced_overwrite_paths.clear)

        result, _moved, output = self.run_operation(
            contexts=(self.intrude_before_quarantine(b"real content, not a stub"),)
        )

        self.assertFalse(bool(result))
        # The new contents survive untouched, and so does the card.
        self.assertEqual(self.dest.read_bytes(), b"real content, not a stub")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertIn("changed after Stackcopy checked it", output)
        # A repair is not an overwrite, and a refused repair is not one either.
        self.assertNotIn("because --force", output)
        self.assertEqual(stackcopy.forced_overwrite_paths, [])
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_the_guarded_file_is_kept_when_it_cannot_be_put_back(self):
        # The destination changes before the guard is taken *and* the name is
        # claimed again before the guard can be restored.  The captured file
        # is the user's, so it is retained and reported rather than deleted.
        real_restore = stackcopy.restore_quarantined_destination

        def restore(guard_path, dest_path):
            write_file(self.dest, b"a third file")
            return real_restore(guard_path, dest_path)

        result, _moved, output = self.run_operation(
            contexts=(
                self.intrude_before_quarantine(b"real content, not a stub"),
                mock.patch.object(
                    stackcopy,
                    "restore_quarantined_destination",
                    side_effect=restore,
                ),
            )
        )

        self.assertFalse(bool(result))
        self.assertEqual(self.dest.read_bytes(), b"a third file")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        guards = sidecars(self.dest_dir)
        self.assertEqual(len(guards), 1, guards)
        self.assertEqual(
            (self.dest_dir / guards[0]).read_bytes(), b"real content, not a stub"
        )
        self.assertIn("was not deleted", output)

    def test_an_unchanged_zero_byte_destination_is_still_repaired(self):
        stackcopy.forced_overwrite_paths.clear()
        self.addCleanup(stackcopy.forced_overwrite_paths.clear)

        result, _moved, output = self.run_operation()

        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.SUCCESS
        )
        self.assertIn("exists but is 0 bytes", output)
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertFalse(self.src.exists())
        self.assertFalse(stackcopy.forced_overwrite_of(result))
        self.assertEqual(stackcopy.forced_overwrite_paths, [])
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_a_zero_byte_repair_by_copy_is_still_repaired(self):
        result, _moved, output = self.run_operation("copy")

        self.assertTrue(bool(result))
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(self.dest_dir), [])


# ---------------------------------------------------------------------------
# C - the destination was identical, so the move would drop the source
# ---------------------------------------------------------------------------


class IdenticalDestinationRaceTests(OperationHarness):
    def setUp(self):
        super().setUp()
        write_file(self.dest, b"irreplaceable", datetime(2026, 8, 24, 12))
        self.other = self.root / "other.ORF"
        write_file(self.other, b"a completely different photo")

    def swap_destination_before_pin(self):
        """Replace the destination just before it is pinned."""
        real_link = stackcopy.os.link
        fired = {"done": False}

        def link(src, dst, **kwargs):
            if not fired["done"] and os.fspath(src) == str(self.dest):
                fired["done"] = True
                os.replace(self.other, self.dest)
            return real_link(src, dst, **kwargs)

        return mock.patch.object(stackcopy.os, "link", side_effect=link)

    def swap_destination_before_source_delete(self):
        """Replace the destination in the instant the source is deleted."""
        real_unlink = stackcopy.os.unlink
        fired = {"done": False}

        def unlink(path, **kwargs):
            if not fired["done"] and os.fspath(path) == str(self.src):
                fired["done"] = True
                os.replace(self.other, self.dest)
            return real_unlink(path, **kwargs)

        return mock.patch.object(stackcopy.os, "unlink", side_effect=unlink)

    def test_a_destination_swapped_before_verification_keeps_the_source(self):
        result, _moved, output = self.run_operation(
            contexts=(self.swap_destination_before_pin(),)
        )

        self.assertFalse(bool(result))
        # The card still holds the photo; nothing was deleted on a stale check.
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(self.dest.read_bytes(), b"a completely different photo")
        self.assertIn("changed after Stackcopy compared it", output)
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_a_destination_swapped_during_deletion_still_keeps_the_photo(self):
        result, _moved, output = self.run_operation(
            contexts=(self.swap_destination_before_source_delete(),)
        )

        # Not a clean move: Stackcopy could not guarantee one.
        self.assertFalse(bool(result))
        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.FAILED
        )
        self.assertEqual(self.dest.read_bytes(), b"a completely different photo")
        # The verified photo survives under the pin, and the run says where.
        pins = sidecars(self.dest_dir)
        self.assertEqual(len(pins), 1, pins)
        self.assertEqual((self.dest_dir / pins[0]).read_bytes(), b"irreplaceable")
        self.assertIn("The verified photo was not", output)
        self.assertIn(pins[0], output)

    def test_an_unchanged_identical_destination_still_empties_the_card(self):
        result, _moved, output = self.run_operation()

        self.assertEqual(
            stackcopy.operation_outcome_of(result), stackcopy.OperationOutcome.SUCCESS
        )
        self.assertIn("already exists with identical content", output)
        self.assertIn("deleted source", output)
        self.assertFalse(self.src.exists())
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_an_unchanged_identical_destination_makes_a_copy_a_no_op(self):
        before = self.dest.stat().st_mtime_ns
        result, moved, output = self.run_operation("copy")

        self.assertTrue(bool(result))
        self.assertEqual(moved, 0)
        self.assertIn("skipping copy from", output)
        self.assertEqual(self.dest.stat().st_mtime_ns, before)
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_a_source_that_cannot_be_deleted_is_reported_not_claimed(self):
        real_unlink = stackcopy.os.unlink
        stackcopy.source_remains_paths.clear()
        self.addCleanup(stackcopy.source_remains_paths.clear)

        def unlink(path, **kwargs):
            if os.fspath(path) == str(self.src):
                raise PermissionError(errno.EACCES, "read-only card")
            return real_unlink(path, **kwargs)

        result, _moved, output = self.run_operation(
            contexts=(mock.patch.object(stackcopy.os, "unlink", side_effect=unlink),)
        )

        self.assertEqual(
            stackcopy.operation_outcome_of(result),
            stackcopy.OperationOutcome.COPIED_SOURCE_REMAINS,
        )
        self.assertEqual(stackcopy.source_remains_paths, [str(self.src)])
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")

    def test_the_source_is_kept_when_the_destination_cannot_be_pinned(self):
        stackcopy.source_remains_paths.clear()
        self.addCleanup(stackcopy.source_remains_paths.clear)

        result, _moved, output = self.run_operation(
            contexts=(
                mock.patch.object(
                    stackcopy.os,
                    "link",
                    side_effect=OSError(errno.EPERM, "no hard links here"),
                ),
            )
        )

        # Conservative: without a pin there is no proof, so nothing is deleted.
        self.assertEqual(
            stackcopy.operation_outcome_of(result),
            stackcopy.OperationOutcome.COPIED_SOURCE_REMAINS,
        )
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertIn("cannot hold it still", output)
        self.assertEqual(sidecars(self.dest_dir), [])


# ---------------------------------------------------------------------------
# CONFLICT + --force
# ---------------------------------------------------------------------------


class ForcedOverwriteTests(OperationHarness):
    def setUp(self):
        super().setUp()
        write_file(self.dest, b"an older, different raw", datetime(2020, 1, 1, 9))
        stackcopy.forced_overwrite_paths.clear()
        self.addCleanup(stackcopy.forced_overwrite_paths.clear)

    def test_force_overwrites_and_tallies_exactly_once(self):
        result, _moved, output = self.run_operation(force=True)

        self.assertTrue(bool(result))
        self.assertTrue(stackcopy.forced_overwrite_of(result))
        self.assertEqual(stackcopy.forced_overwrite_paths, [str(self.dest)])
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertFalse(self.src.exists())
        self.assertIn("Overwriting differing existing file because --force", output)
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_without_force_a_conflict_still_refuses(self):
        result, _moved, output = self.run_operation()

        self.assertFalse(bool(result))
        self.assertEqual(self.dest.read_bytes(), b"an older, different raw")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertIn("Use --force to overwrite", output)
        self.assertEqual(stackcopy.forced_overwrite_paths, [])
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_force_will_not_destroy_a_file_that_changed_after_the_check(self):
        result, _moved, output = self.run_operation(
            force=True,
            contexts=(self.intrude_before_quarantine(b"a brand new photo"),),
        )

        self.assertFalse(bool(result))
        self.assertEqual(self.dest.read_bytes(), b"a brand new photo")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(stackcopy.forced_overwrite_paths, [])
        self.assertIn("changed after Stackcopy checked it", output)
        self.assertEqual(sidecars(self.dest_dir), [])

    def test_a_destination_that_vanishes_is_not_tallied_as_an_overwrite(self):
        real_quarantine = stackcopy.quarantine_destination

        def quarantine(dest_path):
            os.unlink(self.dest)
            return real_quarantine(dest_path)

        result, _moved, output = self.run_operation(
            force=True,
            contexts=(
                mock.patch.object(
                    stackcopy, "quarantine_destination", side_effect=quarantine
                ),
            ),
        )

        self.assertTrue(bool(result))
        self.assertFalse(stackcopy.forced_overwrite_of(result))
        self.assertEqual(stackcopy.forced_overwrite_paths, [])
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(self.dest_dir), [])


# ---------------------------------------------------------------------------
# Interruption
# ---------------------------------------------------------------------------


class InterruptionTests(OperationHarness):
    def test_ctrl_c_during_a_replacement_puts_the_destination_back(self):
        write_file(self.dest, b"")
        real_rename = stackcopy.atomic_rename_no_replace

        def rename(src, dst):
            if os.fspath(dst) == str(self.dest) and not os.path.basename(
                os.fspath(src)
            ).startswith(stackcopy._GUARD_PREFIX):
                raise KeyboardInterrupt
            return real_rename(src, dst)

        with self.assertRaises(KeyboardInterrupt):
            self.run_operation(
                contexts=(
                    mock.patch.object(
                        stackcopy, "atomic_rename_no_replace", side_effect=rename
                    ),
                )
            )

        # The zero-byte file it borrowed is back where it was, the card is
        # untouched, and no guard was abandoned.
        self.assertTrue(self.dest.exists())
        self.assertEqual(self.dest.read_bytes(), b"")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(self.dest_dir), [])


# ---------------------------------------------------------------------------
# The primitives themselves
# ---------------------------------------------------------------------------


class NoReplacePrimitiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.src = self.root / "src"
        self.dest = self.root / "dest"
        write_file(self.src, b"source")

    def mechanisms(self):
        return (
            stackcopy._native_rename_no_replace,
            stackcopy._hard_link_rename_no_replace,
        )

    def test_every_mechanism_refuses_to_clobber(self):
        write_file(self.dest, b"existing")
        for mechanism in self.mechanisms():
            with self.subTest(mechanism=mechanism.__name__):
                try:
                    with self.assertRaises(FileExistsError):
                        mechanism(str(self.src), str(self.dest))
                except stackcopy.NoReplaceUnsupported:
                    self.skipTest("not available on this filesystem")
                self.assertEqual(self.dest.read_bytes(), b"existing")
                self.assertEqual(self.src.read_bytes(), b"source")

    def test_every_mechanism_moves_onto_a_free_name(self):
        for index, mechanism in enumerate(self.mechanisms()):
            with self.subTest(mechanism=mechanism.__name__):
                source = self.root / f"src{index}"
                target = self.root / f"dest{index}"
                write_file(source, b"source")
                try:
                    mechanism(str(source), str(target))
                except stackcopy.NoReplaceUnsupported:
                    self.skipTest("not available on this filesystem")
                self.assertEqual(target.read_bytes(), b"source")
                self.assertFalse(source.exists())

    def test_the_public_primitive_refuses_to_clobber(self):
        write_file(self.dest, b"existing")
        with self.assertRaises(FileExistsError):
            stackcopy.atomic_rename_no_replace(str(self.src), str(self.dest))
        self.assertEqual(self.dest.read_bytes(), b"existing")

    def test_it_falls_through_to_the_next_mechanism(self):
        # A filesystem with no rename flags still gets a no-clobber commit.
        unsupported = stackcopy.NoReplaceUnsupported(errno.EINVAL, "no flags here")
        with mock.patch.object(
            stackcopy, "_native_rename_no_replace", side_effect=unsupported
        ):
            mechanism = stackcopy.atomic_rename_no_replace(
                str(self.src), str(self.dest)
            )
        self.assertEqual(mechanism, stackcopy.NoReplaceMechanism.HARD_LINK)
        self.assertEqual(self.dest.read_bytes(), b"source")

    def test_it_fails_closed_without_native_rename_or_hard_links(self):
        # An exclusive-create reservation cannot safely be exchanged for the
        # source, so FAT/exFAT must fail rather than risk replacing a racer.
        unsupported = stackcopy.NoReplaceUnsupported(errno.EPERM, "not here")
        with ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(
                    stackcopy, "_native_rename_no_replace", side_effect=unsupported
                )
            )
            stack.enter_context(
                mock.patch.object(
                    stackcopy, "_hard_link_rename_no_replace", side_effect=unsupported
                )
            )
            replace_mock = stack.enter_context(
                mock.patch.object(stackcopy.os, "replace")
            )
            with self.assertRaises(OSError) as caught:
                stackcopy.atomic_rename_no_replace(str(self.src), str(self.dest))
        self.assertEqual(caught.exception.errno, errno.ENOSYS)
        replace_mock.assert_not_called()
        self.assertFalse(self.dest.exists())
        self.assertEqual(self.src.read_bytes(), b"source")

    def test_it_fails_closed_when_nothing_is_available(self):
        # There is deliberately no clobbering last resort.
        unsupported = stackcopy.NoReplaceUnsupported(errno.ENOSYS, "nothing here")
        with ExitStack() as stack:
            for name in (
                "_native_rename_no_replace",
                "_hard_link_rename_no_replace",
            ):
                stack.enter_context(
                    mock.patch.object(stackcopy, name, side_effect=unsupported)
                )
            with self.assertRaises(OSError) as caught:
                stackcopy.atomic_rename_no_replace(str(self.src), str(self.dest))
        self.assertEqual(caught.exception.errno, errno.ENOSYS)
        self.assertFalse(self.dest.exists())
        self.assertEqual(self.src.read_bytes(), b"source")

    def test_a_real_error_is_not_mistaken_for_an_unsupported_mechanism(self):
        # ENOSPC means "this failed", not "try the next trick".
        with mock.patch.object(
            stackcopy.os, "link", side_effect=OSError(errno.ENOSPC, "full")
        ):
            with self.assertRaises(OSError) as caught:
                stackcopy._hard_link_rename_no_replace(str(self.src), str(self.dest))
        self.assertEqual(caught.exception.errno, errno.ENOSPC)

    def test_cross_device_is_reported_as_exdev_not_as_unsupported(self):
        with mock.patch.object(
            stackcopy.os, "link", side_effect=OSError(errno.EXDEV, "other fs")
        ):
            with self.assertRaises(OSError) as caught:
                stackcopy._hard_link_rename_no_replace(str(self.src), str(self.dest))
        self.assertEqual(caught.exception.errno, errno.EXDEV)

    def test_failed_source_unlink_never_deletes_a_replacement_destination(self):
        other = self.root / "other"
        write_file(other, b"unrelated")
        real_unlink = stackcopy.os.unlink

        def unlink(path, **kwargs):
            if os.fspath(path) == str(self.src):
                os.replace(other, self.dest)
                raise PermissionError(errno.EACCES, "source is read-only")
            return real_unlink(path, **kwargs)

        with mock.patch.object(
            stackcopy.os, "unlink", side_effect=unlink
        ), self.assertRaises(stackcopy.HardLinkSourceRemovalError):
            stackcopy._hard_link_rename_no_replace(str(self.src), str(self.dest))

        self.assertEqual(self.dest.read_bytes(), b"unrelated")
        self.assertEqual(self.src.read_bytes(), b"source")

    def test_errno_translation(self):
        cases = {
            errno.EEXIST: FileExistsError,
            errno.ENOENT: FileNotFoundError,
            errno.EINVAL: stackcopy.NoReplaceUnsupported,
            errno.ENOSYS: stackcopy.NoReplaceUnsupported,
            errno.EACCES: OSError,
            errno.EIO: OSError,
        }
        for code, expected in cases.items():
            with self.subTest(code=code):
                error = stackcopy._errno_to_oserror(code, "src", "dest")
                self.assertIsInstance(error, expected)
                self.assertEqual(error.errno, code)
        # EACCES is a refusal, not a missing feature: it must never fall
        # through to another mechanism.
        self.assertNotIsInstance(
            stackcopy._errno_to_oserror(errno.EACCES, "src", "dest"),
            stackcopy.NoReplaceUnsupported,
        )

    def test_quarantine_reports_an_absent_destination(self):
        self.assertIsNone(stackcopy.quarantine_destination(str(self.dest)))

    def test_quarantine_moves_the_file_aside_without_changing_it(self):
        write_file(self.dest, b"existing")
        fingerprint = stackcopy._file_fingerprint(str(self.dest))
        guard = stackcopy.quarantine_destination(str(self.dest))

        self.assertIsNotNone(guard)
        self.assertFalse(self.dest.exists())
        self.assertEqual(Path(guard).read_bytes(), b"existing")
        # Identity and state survive the move, which is what lets the caller
        # prove it captured the file it planned to replace.
        self.assertEqual(stackcopy._file_fingerprint(guard), fingerprint)
        self.assertTrue(
            stackcopy.restore_quarantined_destination(guard, str(self.dest))
        )
        self.assertEqual(self.dest.read_bytes(), b"existing")

    def test_a_guard_that_cannot_go_home_is_kept(self):
        write_file(self.dest, b"existing")
        guard = stackcopy.quarantine_destination(str(self.dest))
        write_file(self.dest, b"claimed by somebody else")

        output = io.StringIO()
        with redirect_stdout(output):
            stackcopy.release_quarantined_destination(
                guard, str(self.dest), "moving test file"
            )

        self.assertTrue(os.path.exists(guard))
        self.assertEqual(Path(guard).read_bytes(), b"existing")
        self.assertEqual(self.dest.read_bytes(), b"claimed by somebody else")
        self.assertIn("was not deleted", output.getvalue())


class ReadOnlyDestinationDirectoryTests(OperationHarness):
    @unittest.skipIf(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        "root ignores directory permission bits",
    )
    def test_a_commit_into_a_read_only_directory_fails_closed(self):
        os.chmod(self.dest_dir, stat.S_IRUSR | stat.S_IXUSR)
        self.addCleanup(os.chmod, self.dest_dir, stat.S_IRWXU)

        result, _moved, _output = self.run_operation()

        self.assertFalse(bool(result))
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")


# ---------------------------------------------------------------------------
# The same guarantees through a whole --lightroomimport run
# ---------------------------------------------------------------------------


def run_import(args, *, lightroom, stack_input, contexts=()):
    """Drive main() the way the other suites do, and capture the summary."""
    output = io.StringIO()
    exit_code = 0
    stackcopy._confirmed_filesystems.clear()
    environment = {
        "STACKCOPY_LIGHTROOM_IMPORT_DIR": str(lightroom),
        "STACKCOPY_ASSUME_YES": "1",
    }
    with ExitStack() as stack:
        stack.enter_context(mock.patch.dict(os.environ, environment, clear=False))
        stack.enter_context(
            mock.patch.object(stackcopy, "STACK_INPUT_DIR", str(stack_input))
        )
        stack.enter_context(mock.patch.object(sys, "argv", ["stackcopy.py", *args]))
        stack.enter_context(
            mock.patch.object(stackcopy, "read_stacked_image_metadata", return_value={})
        )
        for context in contexts:
            stack.enter_context(context)
        try:
            with redirect_stdout(output):
                stackcopy.main()
        except SystemExit as error:
            exit_code = error.code if isinstance(error.code, int) else 1
    return exit_code, output.getvalue()


class WholeRunRaceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.card = self.root / "card"
        self.lightroom = self.root / "Lightroom"
        self.stack_input = self.root / "StackInput"
        self.src = self.card / "P8081868.ORF"
        write_file(self.src, b"irreplaceable", datetime(2026, 8, 24, 12))
        self.dest = self.lightroom / "2026" / "2026-08-24" / "P8081868.ORF"

    def go(self, *args, contexts=()):
        return run_import(
            ["--lightroomimport", str(self.card), *args],
            lightroom=self.lightroom,
            stack_input=self.stack_input,
            contexts=contexts,
        )

    def test_a_destination_that_appears_mid_run_is_a_reported_failure(self):
        real_rename = stackcopy.atomic_rename_no_replace
        fired = {"done": False}

        def rename(src, dst):
            if not fired["done"] and os.fspath(dst) == str(self.dest):
                fired["done"] = True
                write_file(self.dest, b"somebody else's photo")
            return real_rename(src, dst)

        code, output = self.go(
            contexts=(
                mock.patch.object(
                    stackcopy, "atomic_rename_no_replace", side_effect=rename
                ),
            )
        )

        # Degraded, and honest about it.
        self.assertEqual(code, 1, output)
        self.assertIn("Failures: 1", output)
        self.assertEqual(self.dest.read_bytes(), b"somebody else's photo")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(self.dest.parent), [])

    def test_an_ordinary_run_still_succeeds_and_leaves_nothing_behind(self):
        code, output = self.go()

        self.assertEqual(code, 0, output)
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertFalse(self.src.exists())
        self.assertEqual(sidecars(self.dest.parent), [])

    def test_a_repeated_import_is_idempotent_and_empties_the_card(self):
        first_code, first = self.go()
        self.assertEqual(first_code, 0, first)
        write_file(self.src, b"irreplaceable", datetime(2026, 8, 24, 12))

        code, output = self.go()

        self.assertEqual(code, 0, output)
        self.assertIn("already exists with identical content", output)
        self.assertFalse(self.src.exists())
        # No duplicate, no suffix, no leftovers.
        self.assertEqual(
            sorted(item.name for item in self.dest.parent.iterdir()),
            ["P8081868.ORF"],
        )

    def test_force_overwrites_a_conflict_and_reports_exactly_one(self):
        write_file(self.dest, b"an older, different raw", datetime(2020, 1, 1, 9))

        code, output = self.go("--force")

        self.assertEqual(code, 0, output)
        self.assertEqual(self.dest.read_bytes(), b"irreplaceable")
        self.assertEqual(output.count("Overwriting differing existing file"), 1)
        self.assertIn("Existing files overwritten by --force: 1", output)
        self.assertEqual(sidecars(self.dest.parent), [])

    def test_companions_keep_one_suffix_when_a_conflict_forces_a_rename(self):
        # A genuine camera filename collision renames the whole stem together;
        # the no-clobber commit must not tempt anything into splitting them.
        write_file(self.card / "P8081868.JPG", b"new jpg", datetime(2026, 8, 24, 12))
        write_file(self.dest, b"different raw", datetime(2020, 1, 1, 9))
        write_file(
            self.dest.with_suffix(".JPG"), b"different jpg", datetime(2020, 1, 1, 9)
        )

        code, output = self.go()

        self.assertEqual(code, 0, output)
        landed = sorted(item.name for item in self.dest.parent.iterdir())
        self.assertEqual(
            landed,
            [
                "P8081868.JPG",
                "P8081868.ORF",
                "P8081868__2.JPG",
                "P8081868__2.ORF",
            ],
        )
        self.assertEqual(
            (self.dest.parent / "P8081868__2.ORF").read_bytes(), b"irreplaceable"
        )
        self.assertEqual(
            (self.dest.parent / "P8081868__2.JPG").read_bytes(), b"new jpg"
        )

    def test_a_race_never_re_suffixes_and_splits_companions(self):
        # A stem's JPG and RAW are deliberately planned with one shared
        # suffix.  If one of them loses a commit race, the answer is to fail
        # that file - never to quietly pick a different suffix for it, which
        # would file the two halves of one photo under different names.
        write_file(self.card / "P8081868.JPG", b"new jpg", datetime(2026, 8, 24, 12))
        dest_dir = self.dest.parent
        write_file(self.dest, b"different raw", datetime(2020, 1, 1, 9))
        write_file(dest_dir / "P8081868.JPG", b"different jpg", datetime(2020, 1, 1, 9))
        intruded = dest_dir / "P8081868__2.ORF"

        real_rename = stackcopy.atomic_rename_no_replace
        fired = {"done": False}

        def rename(src, dst):
            if not fired["done"] and os.fspath(dst) == str(intruded):
                fired["done"] = True
                write_file(intruded, b"somebody else's raw")
            return real_rename(src, dst)

        code, output = self.go(
            contexts=(
                mock.patch.object(
                    stackcopy, "atomic_rename_no_replace", side_effect=rename
                ),
            )
        )

        self.assertEqual(code, 1, output)
        landed = sorted(item.name for item in dest_dir.iterdir())
        # The companion that made it kept the planned suffix, and nothing was
        # filed under a newly invented one.
        self.assertIn("P8081868__2.JPG", landed)
        self.assertFalse([name for name in landed if "__3" in name], landed)
        # The intruder is untouched and the unplaced source is still on the card.
        self.assertEqual(intruded.read_bytes(), b"somebody else's raw")
        self.assertEqual(self.src.read_bytes(), b"irreplaceable")
        self.assertEqual(sidecars(dest_dir), [])


class WindowsErrorTranslationTests(unittest.TestCase):
    """MoveFileExW's codes, checked from any platform.

    The Windows path cannot run here, but the decisions it makes - is this a
    lost race, a cross-volume move, or a real error? - can be.
    """

    def test_an_existing_destination_is_a_lost_race(self):
        for code in (stackcopy._ERROR_ALREADY_EXISTS, stackcopy._ERROR_FILE_EXISTS):
            with self.subTest(code=code):
                error = stackcopy._windows_error_to_oserror(code, "src", "dest")
                self.assertIsInstance(error, FileExistsError)
                self.assertEqual(error.errno, errno.EEXIST)

    def test_a_cross_volume_move_becomes_exdev(self):
        # Without this the durable copy-then-delete path would never run on
        # Windows, and a cross-volume move would skip its fsync barrier.
        error = stackcopy._windows_error_to_oserror(
            stackcopy._ERROR_NOT_SAME_DEVICE, "src", "dest"
        )
        self.assertEqual(error.errno, errno.EXDEV)

    def test_a_missing_source_becomes_filenotfound(self):
        for code in (
            stackcopy._ERROR_FILE_NOT_FOUND,
            stackcopy._ERROR_PATH_NOT_FOUND,
        ):
            with self.subTest(code=code):
                error = stackcopy._windows_error_to_oserror(code, "src", "dest")
                self.assertIsInstance(error, FileNotFoundError)

    def test_anything_else_is_left_to_ctypes(self):
        self.assertIsNone(stackcopy._windows_error_to_oserror(5, "src", "dest"))


class PlatformWithoutNativeRenameTests(unittest.TestCase):
    def test_an_unknown_platform_reports_no_native_rename(self):
        stackcopy._native_rename_no_replace_impl = "unresolved"
        self.addCleanup(
            setattr, stackcopy, "_native_rename_no_replace_impl", "unresolved"
        )
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(stackcopy, "IS_WINDOWS", False))
            stack.enter_context(mock.patch.object(stackcopy, "IS_MACOS", False))
            stack.enter_context(mock.patch.object(sys, "platform", "aix7"))
            self.assertFalse(stackcopy.native_rename_no_replace_available())

    def test_a_broken_libc_binding_is_not_fatal(self):
        stackcopy._native_rename_no_replace_impl = "unresolved"
        self.addCleanup(
            setattr, stackcopy, "_native_rename_no_replace_impl", "unresolved"
        )
        with mock.patch.object(
            stackcopy,
            "_linux_native_rename_no_replace",
            side_effect=OSError("no libc here"),
        ):
            with mock.patch.object(sys, "platform", "linux"):
                with mock.patch.object(stackcopy, "IS_WINDOWS", False):
                    with mock.patch.object(stackcopy, "IS_MACOS", False):
                        self.assertFalse(stackcopy.native_rename_no_replace_available())

    def test_commits_still_work_with_no_native_rename(self):
        stackcopy._native_rename_no_replace_impl = "unresolved"
        self.addCleanup(
            setattr, stackcopy, "_native_rename_no_replace_impl", "unresolved"
        )
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "src"
            target = Path(tmp) / "dest"
            write_file(source, b"payload")
            with ExitStack() as stack:
                stack.enter_context(mock.patch.object(stackcopy, "IS_WINDOWS", False))
                stack.enter_context(mock.patch.object(stackcopy, "IS_MACOS", False))
                stack.enter_context(mock.patch.object(sys, "platform", "aix7"))
                mechanism = stackcopy.atomic_rename_no_replace(str(source), str(target))
            self.assertEqual(mechanism, stackcopy.NoReplaceMechanism.HARD_LINK)
            self.assertEqual(target.read_bytes(), b"payload")


if __name__ == "__main__":
    unittest.main()
