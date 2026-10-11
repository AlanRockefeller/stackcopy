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
            self.assertEqual(payload["dest_lightroom_dates"], ["2026-08-25"])
            self.assertEqual(payload["dest_stack_input_dates"], ["2026-08-25"])
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

    def test_plan_payload_names_every_dated_folder_not_just_the_newest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            camera_dir = source / "DCIM" / "100OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"

            write_file(camera_dir / "P8080001.ORF", b"a", datetime(2026, 8, 23, 9))
            write_file(camera_dir / "P8080002.ORF", b"b", datetime(2026, 8, 25, 9))
            write_file(camera_dir / "P8080003.MOV", b"c", datetime(2026, 8, 25, 10))

            code, output = run_main(
                ["--lightroomimport", str(source), "--plan-json"],
                lightroom=lightroom,
                stack_input=stack_input,
            )

            self.assertEqual(code, 0)
            payload = json.loads(output)
            self.assertEqual(payload["dest_dates"], ["2026-08-23", "2026-08-25"])
            self.assertEqual(payload["dest_lightroom_dates"], ["2026-08-23", "2026-08-25"])
            self.assertEqual(payload["dest_stack_input_dates"], [])
            self.assertEqual(
                payload["dest_dirs"],
                [
                    str(lightroom / "2026" / "2026-08-23"),
                    str(lightroom / "2026" / "2026-08-25"),
                ],
            )
            # The headline destination stays the newest date, so the extra
            # folder is only visible because the full list is reported.
            self.assertEqual(
                payload["dest_lightroom"], str(lightroom / "2026" / "2026-08-25")
            )

    def test_dest_dates_follow_the_planned_folder_not_each_files_mtime(self):
        """A stem straddling midnight lands in one folder and reports one date.

        The RAW is the authority for the whole stem, so its JPG and ORI
        companions go to the RAW's date folder even though their own mtimes
        fall on the next day.  Reporting their mtimes would claim two dates
        for a single dated folder.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            camera_dir = source / "DCIM" / "100OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"

            write_file(
                camera_dir / "P8080001.ORF", b"raw", datetime(2026, 8, 24, 23, 59, 59)
            )
            write_file(
                camera_dir / "P8080001.JPG", b"jpg", datetime(2026, 8, 25, 0, 0, 0)
            )
            write_file(
                camera_dir / "P8080001.ORI", b"ori", datetime(2026, 8, 25, 0, 0, 0)
            )

            code, output = run_main(
                ["--lightroomimport", str(source), "--plan-json"],
                lightroom=lightroom,
                stack_input=stack_input,
            )

            self.assertEqual(code, 0)
            payload = json.loads(output)
            self.assertEqual(
                payload["dest_dirs"], [str(lightroom / "2026" / "2026-08-24")]
            )
            self.assertEqual(payload["dest_dates"], ["2026-08-24"])

    def test_plan_json_reports_scan_progress_on_stderr_only(self):
        # The GUI shows what a plan scan is finding while it runs, so scan
        # progress goes to stderr; stdout must still be exactly one JSON object.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "P8080001.ORF", b"raw", datetime(2026, 8, 25))
            stderr = io.StringIO()

            with mock.patch.object(stackcopy, "_PROGRESS_ENABLED", True):
                code, output = run_main(
                    ["--lightroomimport", str(source), "--plan-json"],
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata={},
                    extra_contexts=(redirect_stderr(stderr),),
                )

            self.assertEqual(code, 0)
            self.assertIsNotNone(json.loads(output))
            self.assertNotIn(stackcopy._PROGRESS_SENTINEL, output)
            progress = [
                line
                for line in stderr.getvalue().splitlines()
                if line.startswith(stackcopy._PROGRESS_SENTINEL)
            ]
            self.assertIn("phase=scan ", progress[0] + " ")
            self.assertIn("@@SCPROGRESS phase=scanned files=1 other=0", progress)
            self.assertFalse(any("phase=prepare" in line for line in progress))

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
            # Progress tracks sizes captured before source files disappear.
            start = next(line for line in progress if "phase=start" in line)
            finished = next(line for line in progress if "phase=done" in line)
            self.assertIn("bytes_total=44", start)
            self.assertIn("bytes_done=0", start)
            self.assertIn("bytes_total=44", finished)
            self.assertIn("bytes_done=44", finished)
            self.assertFalse(list(source.glob("*.*")))
            stack_lines = [line for line in progress if "role=stack_input" in line]
            self.assertTrue(
                all(
                    "stack_output_name=P8080004%20stacked.JPG" in line
                    for line in stack_lines
                )
            )

    def test_byte_progress_counts_skips_failures_and_preview_as_processed_work(self):
        for mode in ("copy", "repeat", "failure", "preview", "interrupted"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                when = datetime(2026, 8, 25, 12)
                write_file(source / "P8080001.JPG", b"photo", when)
                write_file(source / "P8080001.ORF", b"raw-original", when)
                args = ["--lightroomimport", str(source), "--leave-on-card"]
                if mode == "preview":
                    args.append("--dry")
                if mode == "repeat":
                    code, _ = run_main(
                        args, lightroom=lightroom, stack_input=stack_input, metadata={}
                    )
                    self.assertEqual(code, 0)
                stderr = io.StringIO()
                contexts = [redirect_stderr(stderr)]
                if mode == "failure":
                    contexts.append(mock.patch.object(
                        stackcopy, "safe_file_operation", return_value=(False, 0)
                    ))
                if mode == "interrupted":
                    operation = stackcopy.safe_file_operation
                    calls = 0

                    def interrupt_second_file(*args, **kwargs):
                        nonlocal calls
                        calls += 1
                        if calls == 2:
                            raise KeyboardInterrupt
                        return operation(*args, **kwargs)

                    contexts.append(mock.patch.object(
                        stackcopy, "safe_file_operation", side_effect=interrupt_second_file
                    ))
                with mock.patch.object(stackcopy, "_PROGRESS_ENABLED", True):
                    run_main(
                        args, lightroom=lightroom, stack_input=stack_input,
                        metadata={}, extra_contexts=tuple(contexts),
                    )
                progress = [
                    dict(part.split("=", 1) for part in line.split()[1:])
                    for line in stderr.getvalue().splitlines()
                    if line.startswith(stackcopy._PROGRESS_SENTINEL)
                ]
                events = [event for event in progress if event["phase"] == "copy"]
                self.assertEqual([int(event["bytes_done"]) for event in events], [0, 5])
                terminal = "interrupted" if mode == "interrupted" else "done"
                finished = next(event for event in progress if event["phase"] == terminal)
                self.assertEqual(int(finished["bytes_done"]), 5 if mode == "interrupted" else 17)
                self.assertEqual(int(finished["bytes_total"]), 17)
                self.assertTrue((source / "P8080001.JPG").exists())
                self.assertTrue((source / "P8080001.ORF").exists())


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

    def test_files_inside_housekeeping_named_directories_are_not_hidden(self):
        for relative_path in (
            Path("PRIVATE") / "AVCHD" / "BDMV" / "STREAM" / "00001.MTS",
            Path("MISC") / "notes.txt",
        ):
            with self.subTest(path=relative_path), tempfile.TemporaryDirectory() as tmp:
                card = Path(tmp) / "card"
                planned = card / "DCIM" / "100OMSYS" / "P8080001.ORF"
                write_file(planned, b"raw", datetime(2026, 8, 25))
                write_file(
                    card / relative_path,
                    b"must remain visible",
                    datetime(2026, 8, 25),
                )

                self.assertFalse(
                    stackcopy.card_would_be_empty_after(str(card), [str(planned)])
                )


if __name__ == "__main__":
    unittest.main()
