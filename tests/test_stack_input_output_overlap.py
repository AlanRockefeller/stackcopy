"""A frame claimed as a stack input is never also treated as a stack output."""

import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402

from test_final_reliability_pass import make_pair, run_main, write_file  # noqa: E402

WHEN = datetime(2026, 8, 8, 11)


def build_overlapping_card(root: Path) -> tuple[Path, Path, Path]:
    """A card where a heuristic output candidate sits inside a real stack.

    P8081910.JPG says in metadata that it is a ten-frame focus stack, so its
    inputs are P8081900-P8081909.  P8081900.JPG happens to be a lone JPG with
    no RAW beside it, which is exactly the shape of a heuristic in-camera
    stacked output - so it is a candidate for both roles at once.
    """
    source = root / "card"
    camera_dir = source / "DCIM" / "100OLYMP"
    for number in list(range(1897, 1900)) + list(range(1901, 1910)):
        make_pair(camera_dir, f"P808{number}", WHEN)
    write_file(camera_dir / "P8081900.JPG", b"lone jpg", WHEN)
    write_file(camera_dir / "P8081910.JPG", b"stacked", WHEN)
    return source, root / "Lightroom", root / "StackInput"


CARD_FILE_COUNT = 3 * 2 + 1 + 9 * 2 + 1

STACK_METADATA = {
    "P8081910.JPG": stackcopy.StackMetadata(
        stackcopy.StackMetadataState.FOCUS_STACK, 10, "9 10"
    )
}


class ClaimedInputIsNotAlsoAnOutputTests(unittest.TestCase):
    def test_import_moves_a_claimed_input_exactly_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, lightroom, stack_input = build_overlapping_card(root)

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=STACK_METADATA,
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("PARTIAL FAILURE", output)
            self.assertNotIn("Error moving", output)

            dated_inputs = stack_input / "2026" / "2026-08-08"
            dated_lightroom = lightroom / "2026" / "2026-08-08"
            # Claimed as an input by the P8081910 stack, so it moves there
            # unrenamed - and it must not also be renamed as an output.
            self.assertTrue((dated_inputs / "P8081900.JPG").exists())
            self.assertEqual(
                sorted(item.name for item in dated_lightroom.glob("*stacked*")),
                ["P8081910 stacked.JPG"],
            )
            moved = list(lightroom.rglob("*.*")) + list(stack_input.rglob("*.*"))
            self.assertEqual(len(moved), CARD_FILE_COUNT)
            self.assertEqual(list(source.rglob("*.*")), [])

    def test_plan_counts_a_claimed_input_exactly_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, lightroom, stack_input = build_overlapping_card(root)

            code, output = run_main(
                ["--lightroomimport", str(source), "--plan-json"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=STACK_METADATA,
            )

            self.assertEqual(code, 0, output)
            payload = json.loads(output)
            self.assertEqual(payload["total"], CARD_FILE_COUNT)
            self.assertEqual(payload["stacks"], 1)
            self.assertEqual(payload["stacked_outputs"], 1)
            # Ten input stems, nine of which also carry an ORF.
            self.assertEqual(payload["stack_inputs"], 19)
            self.assertEqual(payload["others"], 6)

    def test_plan_reports_the_skipped_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, lightroom, stack_input = build_overlapping_card(root)

            code, output = run_main(
                ["--lightroomimport", str(source), "--dry"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=STACK_METADATA,
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Skipped (claimed as input):    1", output)


class RawBackedWarningTests(unittest.TestCase):
    """The RAW+JPG hint survives an earlier mixed-input stack in the folder."""

    def test_mixed_input_stack_does_not_consume_the_folder_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            camera_dir = source / "DCIM" / "100OLYMP"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"

            # Processed first (the walk is reverse-sorted): a rejected
            # stack whose inferred inputs are a mixed set, so nothing is
            # printed for it.
            make_pair(camera_dir, "P8081901", WHEN)
            make_pair(camera_dir, "P8081902", WHEN)
            write_file(camera_dir / "P8081903.JPG", b"jpg only", WHEN)
            write_file(camera_dir / "P8081904.JPG", b"output", WHEN)
            # Processed second: no inferred input is RAW-backed, which is the
            # case the "Enable RAW+JPG" advice exists for.
            for number in range(1801, 1805):
                write_file(camera_dir / f"P808{number}.JPG", b"jpg only", WHEN)

            code, output = run_main(
                ["--lightroomimport", str(source), "--dry"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Enable RAW+JPG for automatic stack sorting", output)


class RemovableSourceTests(unittest.TestCase):
    def test_wsl_windows_drives_are_not_removable(self):
        for path in ("/mnt/c/Users/alan/incoming", "/mnt/d/incoming"):
            with self.subTest(path=path):
                self.assertFalse(stackcopy.source_is_removable(path))

    def test_linux_removable_mount_roots_are_removable(self):
        if stackcopy.IS_WINDOWS or stackcopy.IS_MACOS:
            self.skipTest("Linux mount-point heuristic")
        self.assertTrue(stackcopy.source_is_removable("/media/alan/OMSYSTEM"))
        self.assertTrue(stackcopy.source_is_removable("/run/media/alan/OMSYSTEM"))


if __name__ == "__main__":
    unittest.main()
