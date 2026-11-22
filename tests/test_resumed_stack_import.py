"""Resuming an interrupted import must not split a stack across destinations.

A card that loses power mid-import leaves some of a stack's frames already in
the stack-input folder and the rest (plus, usually, the stacked JPG) on the
card.  Moves run oldest first, so it is the *earliest* frames that are gone.
When the stack was found by the filename/timing heuristic rather than by
camera metadata, the re-run used to see fewer than MIN_STACK_INPUT_FRAMES
frames, reject the stack, and file the rest as ordinary photos.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import stackcopy  # noqa: E402
from test_final_reliability_pass import (  # noqa: E402
    make_pair,
    run_main,
    write_file,
)

BASE = datetime(2026, 8, 24, 12)


def layout(root: Path) -> dict[str, set[str]]:
    """Map each destination tree to the relative paths of the files in it."""
    trees = {}
    for name in ("Lightroom", "StackInput", "card"):
        tree = root / name
        trees[name] = (
            {str(item.relative_to(tree)) for item in tree.rglob("*") if item.is_file()}
            if tree.exists()
            else set()
        )
    return trees


@contextmanager
def card_vanishes_after(successful_moves: int):
    """Let the first N file operations succeed, then fail every later one."""
    real = stackcopy.safe_file_operation
    calls = {"n": 0}

    def flaky(operation, *args, **kwargs):
        if operation in ("move", "copy") and not kwargs.get("dry_run") and not (
            len(args) >= 5 and args[4]
        ):
            calls["n"] += 1
            if calls["n"] > successful_moves:
                return False, 0
        return real(operation, *args, **kwargs)

    with mock.patch.object(stackcopy, "safe_file_operation", side_effect=flaky):
        yield


class ResumedHeuristicStackTests(unittest.TestCase):
    def make_card(self, root: Path, frames: int = 4, *, start: datetime = BASE):
        """A heuristic stack: RAW+JPG frames 1..N, then a JPG-only output."""
        card = root / "card" / "DCIM" / "100OMSYS"
        for number in range(1, frames + 1):
            make_pair(card, f"P808{number:04d}", start + timedelta(seconds=number))
        write_file(
            card / f"P808{frames + 1:04d}.JPG",
            b"stacked output",
            start + timedelta(seconds=frames + 5),
        )
        # An unrelated, later single shot that must stay an ordinary photo.
        make_pair(card, f"P808{frames + 2:04d}", start + timedelta(minutes=10))
        return root / "card"

    def run_import(self, root: Path, *extra):
        return run_main(
            ["--lightroomimport", str(root / "card"), *extra],
            lightroom=root / "Lightroom",
            stack_input=root / "StackInput",
            metadata={},
        )

    def baseline(self, frames: int = 4, **card_kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_card(root, frames, **card_kwargs)
            code, output = self.run_import(root)
            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            return layout(root)

    def assert_resume_matches_baseline(self, frames: int, **card_kwargs):
        expected = self.baseline(frames, **card_kwargs)
        total_files = sum(len(paths) for paths in expected.values())
        for k in range(total_files + 1):
            with self.subTest(successful_moves=k):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    self.make_card(root, frames, **card_kwargs)
                    with card_vanishes_after(k):
                        self.run_import(root)
                    code, output = self.run_import(root)
                    self.assertEqual(code, 0, output)
                    self.assertEqual(layout(root), expected, output)

    def test_every_interruption_point_resumes_to_the_uninterrupted_layout(self):
        self.assert_resume_matches_baseline(frames=4)

    def test_minimum_size_stack_resumes_to_the_uninterrupted_layout(self):
        self.assert_resume_matches_baseline(frames=3)

    def test_stack_shot_across_midnight_resumes(self):
        self.assert_resume_matches_baseline(
            frames=4, start=datetime(2026, 8, 24, 23, 59, 57)
        )

    def test_two_frames_without_earlier_evidence_are_still_rejected(self):
        # Nothing was ever imported: two frames are not a stack, as before.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_card(root, frames=2)
            code, output = self.run_import(root)
            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               0", output)
            self.assertFalse((root / "StackInput").exists())

    def test_stack_input_frames_outside_the_time_window_are_not_evidence(self):
        # Matching names in the stack-input folder from hours earlier are an
        # unrelated stack, not this one's missing frames.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card" / "DCIM" / "100OMSYS"
            for number in (3, 4):
                make_pair(card, f"P808{number:04d}", BASE + timedelta(seconds=number))
            write_file(card / "P8080005.JPG", b"out", BASE + timedelta(seconds=9))
            dated = root / "StackInput" / "2026" / "2026-08-24"
            for number in (1, 2):
                make_pair(dated, f"P808{number:04d}", BASE - timedelta(hours=3))

            code, output = self.run_import(root)

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               0", output)
            self.assertIn("2026/2026-08-24/P8080005.JPG", layout(root)["Lightroom"])
            self.assertNotIn("continues a stack", output)

    def test_jpg_only_frames_in_the_stack_input_folder_are_not_evidence(self):
        # Heuristic stacks need RAW-backed inputs; a stray JPG does not count.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card" / "DCIM" / "100OMSYS"
            for number in (3, 4):
                make_pair(card, f"P808{number:04d}", BASE + timedelta(seconds=number))
            write_file(card / "P8080005.JPG", b"out", BASE + timedelta(seconds=9))
            dated = root / "StackInput" / "2026" / "2026-08-24"
            for number in (1, 2):
                make_pair(
                    dated,
                    f"P808{number:04d}",
                    BASE + timedelta(seconds=number),
                    extensions=("JPG",),
                )

            code, output = self.run_import(root)

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               0", output)

    def test_copy_mode_resume_is_unchanged(self):
        # Leave-on-card never removes frames, so the whole stack is always
        # visible on the card and a second run finds everything identical.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_card(root)
            with card_vanishes_after(3):
                self.run_import(root, "--leave-on-card")
            code, output = self.run_import(root, "--leave-on-card")
            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            self.assertIn(
                "P8080005 stacked.JPG",
                {Path(p).name for p in layout(root)["Lightroom"]},
            )


if __name__ == "__main__":
    unittest.main()
