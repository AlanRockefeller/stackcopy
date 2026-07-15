import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy as stackcopy_module  # noqa: E402
from stackcopy import collect_consecutive_probe_stems  # noqa: E402

STACKCOPY = ROOT / "stackcopy.py"


def write_media_file(path: Path, mtime: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"{path.name}\n".encode("ascii"))
    ts = mtime.timestamp()
    os.utime(path, (ts, ts))


def files_under(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(p.relative_to(path) for p in path.rglob("*") if p.is_file())


class ConsecutiveProbeTests(unittest.TestCase):
    def test_collects_forward_and_backward_until_numeric_discontinuity(self):
        sequence = [
            (10, "frame10"),
            (11, "frame11"),
            (13, "frame13"),
            (14, "frame14"),
        ]

        forward = collect_consecutive_probe_stems(
            sequence,
            start_index=0,
            expected_num=10,
            direction=1,
            required_count=4,
        )
        backward = collect_consecutive_probe_stems(
            sequence,
            start_index=3,
            expected_num=14,
            direction=-1,
            required_count=4,
        )

        self.assertEqual(forward, ("frame10", "frame11"))
        self.assertEqual(backward, ("frame14", "frame13"))

    def test_sequence_bounds_and_short_probe_return_partial_collection(self):
        sequence = [(20, "frame20"), (21, "frame21")]

        before_start = collect_consecutive_probe_stems(
            sequence,
            start_index=-1,
            expected_num=19,
            direction=-1,
            required_count=3,
        )
        after_end = collect_consecutive_probe_stems(
            sequence,
            start_index=len(sequence),
            expected_num=22,
            direction=1,
            required_count=3,
        )
        short_forward = collect_consecutive_probe_stems(
            sequence,
            start_index=0,
            expected_num=20,
            direction=1,
            required_count=3,
        )
        short_backward = collect_consecutive_probe_stems(
            sequence,
            start_index=1,
            expected_num=21,
            direction=-1,
            required_count=3,
        )

        self.assertEqual(before_start, ())
        self.assertEqual(after_end, ())
        self.assertEqual(short_forward, ("frame20", "frame21"))
        self.assertEqual(short_backward, ("frame21", "frame20"))

    def test_probe_arguments_are_validated(self):
        with self.assertRaisesRegex(ValueError, r"direction must be -1 or \+1"):
            collect_consecutive_probe_stems(
                [(1, "frame1")],
                start_index=0,
                expected_num=1,
                direction=0,
                required_count=1,
            )
        with self.assertRaisesRegex(ValueError, "required_count must be non-negative"):
            collect_consecutive_probe_stems(
                [(1, "frame1")],
                start_index=0,
                expected_num=1,
                direction=1,
                required_count=-1,
            )


class LightroomJpgOnlyGuardTests(unittest.TestCase):
    def run_stackcopy(self, args: list[str], lightroom: Path, stack_input: Path):
        env = os.environ.copy()
        env["STACKCOPY_LIGHTROOM_IMPORT_DIR"] = str(lightroom)
        env["STACKCOPY_STACK_INPUT_DIR"] = str(stack_input)
        env["STACKCOPY_ASSUME_YES"] = "1"
        return subprocess.run(
            [sys.executable, str(STACKCOPY), *args],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_version_flag_reports_current_version(self):
        result = subprocess.run(
            [sys.executable, str(STACKCOPY), "--version"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.strip(), "Stackcopy 1.5.8")

    def test_lightroomimport_jpg_only_repro_imports_all_as_remaining(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            camera_dir = src / "DCIM" / "100OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            for i in range(1, 27):
                write_media_file(
                    camera_dir / f"_617{i:04d}.JPG",
                    base_time + timedelta(seconds=i),
                )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src)], lightroom, stack_input
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("JPG-only import detected", result.stdout)
            self.assertIn("Stack detection has been disabled", result.stdout)
            self.assertIn("Stacked JPG candidates found:  0", result.stdout)
            self.assertIn("Will move 0 stacked output files", result.stdout)
            self.assertIn("Will move 0 stack input files", result.stdout)
            self.assertIn("Will move 26 remaining files", result.stdout)
            self.assertIn(
                "Breakdown: 0 stacked outputs, 0 stack inputs, 26 remaining",
                result.stdout,
            )

            imported = files_under(lightroom)
            self.assertEqual(len(imported), 26)
            self.assertEqual(files_under(stack_input), [])
            self.assertFalse(any("stacked" in p.name.lower() for p in imported))
            self.assertEqual(files_under(src), [])

    def test_lightroom_jpg_only_repro_does_not_rename_or_move_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "camera"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            for i in range(1, 27):
                write_media_file(src / f"_617{i:04d}.JPG", base_time)

            result = self.run_stackcopy(
                ["--lightroom", str(src)], lightroom, stack_input
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("JPG-only import detected", result.stdout)
            self.assertIn("Done. Processed 0 stacked JPG files", result.stdout)
            self.assertEqual(len(files_under(src)), 26)
            self.assertEqual(files_under(stack_input), [])
            self.assertFalse(any("stacked" in p.name.lower() for p in files_under(src)))

    def test_lightroomimport_raw_jpg_keeps_in_camera_stack_behavior(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            media_times = {
                1: base_time,
                2: base_time + timedelta(seconds=100),
                3: base_time + timedelta(seconds=102),
                4: base_time + timedelta(seconds=104),
                5: base_time + timedelta(seconds=110),
                6: base_time + timedelta(seconds=220),
            }
            for i in (1, 2, 3, 4, 6):
                write_media_file(src / f"_617{i:04d}.JPG", media_times[i])
                write_media_file(src / f"_617{i:04d}.ORF", media_times[i])
            write_media_file(src / "_6170005.JPG", media_times[5])

            result = self.run_stackcopy(
                ["--lightroomimport", str(src)], lightroom, stack_input
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("JPG-only import detected", result.stdout)
            self.assertIn("Stacked JPG candidates found:  1", result.stdout)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 6 stack inputs, 4 remaining",
                result.stdout,
            )

            lightroom_files = {p.name for p in files_under(lightroom)}
            stack_files = {p.name for p in files_under(stack_input)}
            self.assertIn("_6170005 stacked.JPG", lightroom_files)
            self.assertEqual(
                {"_6170001.JPG", "_6170001.ORF", "_6170006.JPG", "_6170006.ORF"},
                lightroom_files - {"_6170005 stacked.JPG"},
            )
            self.assertEqual(
                {
                    "_6170002.JPG",
                    "_6170002.ORF",
                    "_6170003.JPG",
                    "_6170003.ORF",
                    "_6170004.JPG",
                    "_6170004.ORF",
                },
                stack_files,
            )
            self.assertEqual(files_under(src), [])

    def test_valid_stack_followed_by_quick_ordinary_burst_in_both_modes(self):
        """Following photos never participate in the output candidate decision."""
        for mode in ("--lightroom", "--lightroomimport"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                src = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                base_time = datetime(2026, 7, 14, 12, 0, 0)

                for number in range(1, 7):
                    mtime = base_time + timedelta(milliseconds=number * 100)
                    write_media_file(src / f"_714{number:04d}.JPG", mtime)
                    write_media_file(src / f"_714{number:04d}.ORF", mtime)

                output_time = base_time + timedelta(milliseconds=700)
                write_media_file(src / "_7140007.JPG", output_time)

                following_names = set()
                for number in range(8, 12):
                    mtime = base_time + timedelta(milliseconds=number * 100)
                    for extension in ("JPG", "ORF"):
                        name = f"_714{number:04d}.{extension}"
                        following_names.add(name)
                        write_media_file(src / name, mtime)

                result = self.run_stackcopy(
                    [mode, str(src), "--debug-stacks"], lightroom, stack_input
                )

                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertNotIn("Burst Safety Check: TRIGGERED", result.stdout)
                self.assertIn("Final Decision: ACCEPTED", result.stdout)

                expected_inputs = {
                    f"_714{number:04d}.{extension}"
                    for number in range(1, 7)
                    for extension in ("JPG", "ORF")
                }
                self.assertEqual(
                    expected_inputs,
                    {path.name for path in files_under(stack_input)},
                )

                output_name = "_7140007 stacked.JPG"
                if mode == "--lightroomimport":
                    self.assertIn("Accepted stacks:               1", result.stdout)
                    self.assertIn(
                        "Breakdown: 1 stacked outputs, 12 stack inputs, 8 remaining",
                        result.stdout,
                    )
                    self.assertEqual(
                        following_names | {output_name},
                        {path.name for path in files_under(lightroom)},
                    )
                    self.assertEqual(files_under(src), [])
                else:
                    self.assertEqual(
                        following_names | {output_name},
                        {path.name for path in files_under(src)},
                    )
                    self.assertEqual(files_under(lightroom), [])

    def test_maximum_size_stack_followed_by_rapid_photos_is_accepted(self):
        """Trust the intact camera file model even when all mtimes are continuous."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 7, 14, 13, 0, 0)

            for number in range(1, 16):
                mtime = base_time + timedelta(milliseconds=number * 100)
                write_media_file(src / f"_715{number:04d}.JPG", mtime)
                write_media_file(src / f"_715{number:04d}.ORF", mtime)
            write_media_file(
                src / "_7150016.JPG",
                base_time + timedelta(milliseconds=1600),
            )
            following_names = set()
            for number in range(17, 21):
                mtime = base_time + timedelta(milliseconds=number * 100)
                for extension in ("JPG", "ORF"):
                    name = f"_715{number:04d}.{extension}"
                    following_names.add(name)
                    write_media_file(src / name, mtime)

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("Burst Safety Check: TRIGGERED", result.stdout)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 30 stack inputs, 8 remaining",
                result.stdout,
            )
            self.assertEqual(
                following_names | {"_7150016 stacked.JPG"},
                {path.name for path in files_under(lightroom)},
            )
            self.assertEqual(
                {
                    f"_715{number:04d}.{extension}"
                    for number in range(1, 16)
                    for extension in ("JPG", "ORF")
                },
                {path.name for path in files_under(stack_input)},
            )
            self.assertEqual(files_under(src), [])

    def test_normal_shorter_stack_without_following_photos_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 7, 14, 13, 30, 0)

            for number in range(1, 6):
                mtime = base_time + timedelta(milliseconds=number * 200)
                write_media_file(src / f"_7155{number:03d}.JPG", mtime)
                write_media_file(src / f"_7155{number:03d}.ORF", mtime)
            write_media_file(
                src / "_7155006.JPG",
                base_time + timedelta(milliseconds=1200),
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 10 stack inputs, 0 remaining",
                result.stdout,
            )
            self.assertEqual(
                {"_7155006 stacked.JPG"},
                {path.name for path in files_under(lightroom)},
            )
            self.assertEqual(10, len(files_under(stack_input)))
            self.assertEqual(files_under(src), [])

    def test_lightroomimport_quick_consecutive_stacks_stop_at_prior_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            media_times = {
                1: base_time,
                2: base_time + timedelta(seconds=2),
                3: base_time + timedelta(seconds=4),
                4: base_time + timedelta(seconds=8),
                5: base_time + timedelta(seconds=12),
                6: base_time + timedelta(seconds=14),
                7: base_time + timedelta(seconds=16),
                8: base_time + timedelta(seconds=20),
            }
            for i in (1, 2, 3, 5, 6, 7):
                write_media_file(src / f"_617{i:04d}.JPG", media_times[i])
                write_media_file(src / f"_617{i:04d}.ORF", media_times[i])
            write_media_file(src / "_6170004.JPG", media_times[4])
            write_media_file(src / "_6170008.JPG", media_times[8])

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn(
                "inferred input frames are not all RAW-backed", result.stdout
            )
            self.assertIn(
                "Non-RAW-backed boundary after sufficient inputs", result.stdout
            )
            self.assertIn("Stacked JPG candidates found:  2", result.stdout)
            self.assertIn("Accepted stacks:               2", result.stdout)
            self.assertIn(
                "Breakdown: 2 stacked outputs, 12 stack inputs, 0 remaining",
                result.stdout,
            )

            lightroom_files = {p.name for p in files_under(lightroom)}
            stack_files = {p.name for p in files_under(stack_input)}
            self.assertEqual(
                {"_6170004 stacked.JPG", "_6170008 stacked.JPG"},
                lightroom_files,
            )
            self.assertEqual(
                {
                    "_6170001.JPG",
                    "_6170001.ORF",
                    "_6170002.JPG",
                    "_6170002.ORF",
                    "_6170003.JPG",
                    "_6170003.ORF",
                    "_6170005.JPG",
                    "_6170005.ORF",
                    "_6170006.JPG",
                    "_6170006.ORF",
                    "_6170007.JPG",
                    "_6170007.ORF",
                },
                stack_files,
            )
            self.assertEqual(files_under(src), [])

    def test_lightroomimport_preserves_backward_overlength_safeguard(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 7, 14, 14, 0, 0)

            # Fifteen inferred inputs hit the cap; the three immediately older
            # consecutive frames remain within the original backward gap rule.
            for number in range(1, 19):
                mtime = base_time + timedelta(milliseconds=number * 100)
                write_media_file(src / f"_716{number:04d}.JPG", mtime)
                write_media_file(src / f"_716{number:04d}.ORF", mtime)
            write_media_file(
                src / "_7160019.JPG",
                base_time + timedelta(milliseconds=1900),
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(
                "Burst Safety Check: TRIGGERED. Found 3 extra frames within "
                "2.0s of start.",
                result.stdout,
            )
            self.assertIn("Accepted stacks:               0", result.stdout)
            self.assertIn("Burst safety:              1", result.stdout)
            self.assertIn(
                "Breakdown: 0 stacked outputs, 0 stack inputs, 37 remaining",
                result.stdout,
            )
            imported_names = {path.name for path in files_under(lightroom)}
            self.assertIn("_7160019.JPG", imported_names)
            self.assertNotIn("_7160019 stacked.JPG", imported_names)
            self.assertEqual(files_under(stack_input), [])
            self.assertEqual(files_under(src), [])

    def test_backward_probe_numeric_gap_does_not_trigger_rejection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 7, 14, 14, 30, 0)

            # Frames 5-19 fill the input window. Frames 4 and 3 form only a
            # partial backward probe because frame 2 is absent.
            for number in (1, *range(3, 20)):
                mtime = base_time + timedelta(milliseconds=number * 100)
                write_media_file(src / f"_717{number:04d}.JPG", mtime)
                write_media_file(src / f"_717{number:04d}.ORF", mtime)
            write_media_file(
                src / "_7170020.JPG",
                base_time + timedelta(milliseconds=2000),
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("Burst Safety Check: TRIGGERED", result.stdout)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 30 stack inputs, 6 remaining",
                result.stdout,
            )
            self.assertIn(
                "_7170020 stacked.JPG",
                {path.name for path in files_under(lightroom)},
            )
            self.assertEqual(30, len(files_under(stack_input)))
            self.assertEqual(files_under(src), [])

    def test_missing_backward_probe_mtime_does_not_trigger_rejection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 7, 14, 15, 0, 0)

            for number in range(1, 19):
                mtime = base_time + timedelta(milliseconds=number * 100)
                write_media_file(src / f"_718{number:04d}.JPG", mtime)
                write_media_file(src / f"_718{number:04d}.ORF", mtime)
            write_media_file(
                src / "_7180019.JPG",
                base_time + timedelta(milliseconds=1900),
            )

            original_get_stem_mtime = stackcopy_module.get_stem_mtime

            def get_stem_mtime_with_missing_probe(record, verbose=False):
                raw_record = record["files"].get("raw")
                if raw_record and raw_record["basename"] == "_7180002.ORF":
                    return None
                return original_get_stem_mtime(record, verbose)

            output = io.StringIO()
            env = {
                "STACKCOPY_LIGHTROOM_IMPORT_DIR": str(lightroom),
                "STACKCOPY_STACK_INPUT_DIR": str(stack_input),
                "STACKCOPY_ASSUME_YES": "1",
            }
            with (
                mock.patch.dict(os.environ, env),
                mock.patch.object(
                    stackcopy_module, "STACK_INPUT_DIR", str(stack_input)
                ),
                mock.patch.object(
                    stackcopy_module,
                    "get_stem_mtime",
                    get_stem_mtime_with_missing_probe,
                ),
                mock.patch.object(
                    sys,
                    "argv",
                    [str(STACKCOPY), "--lightroomimport", str(src), "--debug-stacks"],
                ),
                redirect_stdout(output),
            ):
                stackcopy_module.main()

            stdout = output.getvalue()
            self.assertNotIn("Burst Safety Check: TRIGGERED", stdout)
            self.assertIn("Accepted stacks:               1", stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 30 stack inputs, 6 remaining",
                stdout,
            )
            self.assertIn(
                "_7180019 stacked.JPG",
                {path.name for path in files_under(lightroom)},
            )
            self.assertEqual(30, len(files_under(stack_input)))
            self.assertEqual(files_under(src), [])

    def test_lightroomimport_no_stack_detection_imports_all_as_remaining(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            media_times = {
                1: base_time,
                2: base_time + timedelta(seconds=100),
                3: base_time + timedelta(seconds=102),
                4: base_time + timedelta(seconds=104),
                5: base_time + timedelta(seconds=110),
            }
            for i in (1, 2, 3, 4):
                write_media_file(src / f"_617{i:04d}.JPG", media_times[i])
                write_media_file(src / f"_617{i:04d}.ORF", media_times[i])
            write_media_file(src / "_6170005.JPG", media_times[5])

            result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(src),
                    "--no-stack-detection",
                    "--debug-stacks",
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(
                "Stack detection disabled by --no-stack-detection.", result.stdout
            )
            self.assertNotIn("--- Debugging Stack for Output:", result.stdout)
            self.assertIn("Stacked JPG candidates found:  0", result.stdout)
            self.assertIn("Accepted stacks:               0", result.stdout)
            self.assertIn("Will move 0 stacked output files", result.stdout)
            self.assertIn("Will move 0 stack input files", result.stdout)
            self.assertIn("Will move 9 remaining files", result.stdout)
            self.assertIn(
                "Breakdown: 0 stacked outputs, 0 stack inputs, 9 remaining",
                result.stdout,
            )

            imported = files_under(lightroom)
            self.assertEqual(len(imported), 9)
            self.assertEqual(files_under(stack_input), [])
            self.assertFalse(any("stacked" in p.name.lower() for p in imported))
            self.assertEqual(files_under(src), [])

    def test_lightroomimport_mixed_group_keeps_later_jpg_only_as_remaining(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            camera_dir = src / "DCIM" / "100OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)
            later_time = base_time + timedelta(minutes=5)

            for i in range(1, 11):
                mtime = base_time + timedelta(seconds=i * 2)
                write_media_file(camera_dir / f"_617{i:04d}.JPG", mtime)
                write_media_file(camera_dir / f"_617{i:04d}.ORF", mtime)

            for i in range(11, 37):
                write_media_file(
                    camera_dir / f"_617{i:04d}.JPG",
                    later_time + timedelta(seconds=i),
                )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(f"Running Stackcopy from: {STACKCOPY}", result.stdout)
            self.assertNotIn("JPG-only import detected", result.stdout)
            self.assertEqual(result.stdout.count("Stack detection skipped in "), 1)
            self.assertIn(
                "inferred input frames are not all RAW-backed. Enable RAW+JPG for automatic stack sorting.",
                result.stdout,
            )
            self.assertIn(
                "Stack REJECTED: inferred input frames are not all RAW-backed; automatic stack detection requires RAW-backed input frames.",
                result.stdout,
            )
            self.assertIn("Stacked JPG candidates found:  26", result.stdout)
            self.assertIn("Accepted stacks:               0", result.stdout)
            self.assertIn("Input sequences not all RAW-backed skipped:", result.stdout)
            self.assertIn("Will move 0 stacked output files", result.stdout)
            self.assertIn("Will move 0 stack input files", result.stdout)
            self.assertIn("Will move 46 remaining files", result.stdout)
            self.assertIn(
                "Breakdown: 0 stacked outputs, 0 stack inputs, 46 remaining",
                result.stdout,
            )

            imported = files_under(lightroom)
            imported_names = {p.name for p in imported}
            self.assertEqual(len(imported), 46)
            self.assertEqual(files_under(stack_input), [])
            self.assertFalse(any("stacked" in p.name.lower() for p in imported))
            for i in range(11, 37):
                self.assertIn(f"_617{i:04d}.JPG", imported_names)
            self.assertEqual(files_under(src), [])

    def test_lightroomimport_preserves_stack_detection_across_roll_folders(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            previous_roll = src / "DCIM" / "100OMSYS"
            next_roll = src / "DCIM" / "101OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            frame_times = {
                9997: base_time,
                9998: base_time + timedelta(seconds=2),
                9999: base_time + timedelta(seconds=4),
            }
            for number, mtime in frame_times.items():
                write_media_file(previous_roll / f"_617{number:04d}.JPG", mtime)
                write_media_file(previous_roll / f"_617{number:04d}.ORF", mtime)
            write_media_file(
                next_roll / "_6180000.JPG",
                base_time + timedelta(seconds=10),
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src)], lightroom, stack_input
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("JPG-only import detected", result.stdout)
            self.assertIn("Stacked JPG candidates found:  1", result.stdout)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 6 stack inputs, 0 remaining",
                result.stdout,
            )

            lightroom_files = {p.name for p in files_under(lightroom)}
            stack_files = {p.name for p in files_under(stack_input)}
            self.assertEqual({"_6180000 stacked.JPG"}, lightroom_files)
            self.assertEqual(
                {
                    "_6179997.JPG",
                    "_6179997.ORF",
                    "_6179998.JPG",
                    "_6179998.ORF",
                    "_6179999.JPG",
                    "_6179999.ORF",
                },
                stack_files,
            )
            self.assertEqual(files_under(src), [])


if __name__ == "__main__":
    unittest.main()
