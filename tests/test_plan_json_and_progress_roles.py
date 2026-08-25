"""Machine-readable import planning and GUI progress metadata."""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402

from test_final_reliability_pass import run_main, write_file  # noqa: E402


class PlanJsonTests(unittest.TestCase):
    def test_plan_payload_comes_from_real_planned_moves_and_moves_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            camera_dir = source / "DCIM" / "100OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 25, 12)
            expected_bytes = 0

            for number in (1, 2, 3):
                for extension in ("JPG", "ORF"):
                    content = f"{number}-{extension}".encode()
                    expected_bytes += len(content)
                    write_file(
                        camera_dir / f"P808000{number}.{extension}", content, when
                    )
            for name, content in (
                ("P8080004.JPG", b"finished"),
                ("P8080010.ORF", b"single"),
                ("P8080011.MOV", b"video"),
            ):
                expected_bytes += len(content)
                write_file(camera_dir / name, content, when)

            metadata = {
                "P8080004.JPG": stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                )
            }
            code, output = run_main(
                ["--lightroomimport", str(source), "--plan-json"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
            )

            self.assertEqual(code, 0)
            self.assertEqual(len(output.strip().splitlines()), 1, output)
            payload = json.loads(output)
            self.assertEqual(payload["total"], 9)
            self.assertEqual(payload["bytes"], expected_bytes)
            self.assertEqual(payload["stacks"], 1)
            self.assertEqual(payload["stacked_outputs"], 1)
            self.assertEqual(payload["stack_inputs"], 6)
            self.assertEqual(payload["others"], 2)
            self.assertEqual(payload["other_photos"], 1)
            self.assertEqual(payload["other_videos"], 1)
            self.assertEqual(payload["newest_date"], "2026-08-25")
            self.assertEqual(
                payload["dest_lightroom"],
                str(lightroom / "2026" / "2026-08-25"),
            )
            self.assertEqual(
                payload["dest_stack_input"],
                str(stack_input / "2026" / "2026-08-25"),
            )
            self.assertEqual(
                payload["source_subdirs_scanned"],
                [os.path.join("DCIM", "100OMSYS")],
            )
            self.assertFalse(payload["source_is_removable"])
            self.assertTrue(payload["source_would_be_empty_after"])
            self.assertEqual(len(list(source.rglob("*.*"))), 9)
            self.assertFalse(lightroom.exists())
            self.assertFalse(stack_input.exists())

    def test_leave_on_card_plan_does_not_claim_source_will_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "P8080001.ORF", b"raw", datetime(2026, 8, 25))

            code, output = run_main(
                [
                    "--lightroomimport",
                    str(source),
                    "--plan-json",
                    "--leave-on-card",
                ],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0)
            self.assertFalse(json.loads(output)["source_would_be_empty_after"])


class ProgressRoleTests(unittest.TestCase):
    def test_real_import_progress_includes_roles_and_stack_output_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 25, 12)
            for number in (1, 2, 3):
                for extension in ("JPG", "ORF"):
                    write_file(source / f"P808000{number}.{extension}", b"frame", when)
            write_file(source / "P8080004.JPG", b"finished", when)
            write_file(source / "P8080010.ORF", b"single", when)
            metadata = {
                "P8080004.JPG": stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                )
            }
            stderr = io.StringIO()
            with mock.patch.object(stackcopy, "_PROGRESS_ENABLED", True):
                code, _ = run_main(
                    ["--lightroomimport", str(source)],
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata=metadata,
                    extra_contexts=(redirect_stderr(stderr),),
                )

            self.assertEqual(code, 0)
            progress = [
                line
                for line in stderr.getvalue().splitlines()
                if line.startswith(stackcopy._PROGRESS_SENTINEL)
            ]
            self.assertTrue(any("role=stack_output" in line for line in progress))
            self.assertTrue(any("role=stack_input" in line for line in progress))
            self.assertTrue(any("role=other" in line for line in progress))
            self.assertTrue(any("phase=scan" in line for line in progress))
            self.assertTrue(any("phase=prepare" in line for line in progress))
            stack_lines = [line for line in progress if "role=stack_input" in line]
            self.assertTrue(
                all(
                    "stack_output_name=P8080004%20stacked.JPG" in line
                    for line in stack_lines
                )
            )


class CardEmptyHeuristicTests(unittest.TestCase):
    def test_ignores_camera_housekeeping_but_not_an_unplanned_photo(self):
        with tempfile.TemporaryDirectory() as tmp:
            card = Path(tmp) / "card"
            planned = card / "DCIM" / "100OMSYS" / "P8080001.ORF"
            write_file(planned, b"raw", datetime(2026, 8, 25))
            write_file(card / "MISC" / "AUTPRINT.MRK", b"camera", datetime.now())
            write_file(card / ".DS_Store", b"mac", datetime.now())

            self.assertTrue(
                stackcopy.card_would_be_empty_after(str(card), [str(planned)])
            )

            extra = card / "DCIM" / "100OMSYS" / "P8080002.JPG"
            write_file(extra, b"photo", datetime(2026, 8, 25))
            self.assertFalse(
                stackcopy.card_would_be_empty_after(str(card), [str(planned)])
            )


if __name__ == "__main__":
    unittest.main()
