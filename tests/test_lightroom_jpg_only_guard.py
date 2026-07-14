import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
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

            result = self.run_stackcopy(["--lightroom", str(src)], lightroom, stack_input)

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
            self.assertNotIn("inferred input frames are not all RAW-backed", result.stdout)
            self.assertIn("Non-RAW-backed boundary after sufficient inputs", result.stdout)
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

    def test_lightroomimport_rejects_stack_candidate_inside_continuing_bracket(self):
        """A missing RAW in a long bracket must not look like a stack output.

        This mirrors the P6292502-P6292522 portion of the June 29, 2026
        regression.  The candidate at P6292519 has 15 valid-looking inputs
        behind it, while P6292501 is an earlier, genuine stack output separated
        by five minutes.  A backward-only burst probe is fooled by that older
        output; the frames after P6292519 prove that the bracket is continuing.
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            stack_time = datetime(2026, 6, 29, 11, 35, 28)
            bracket_time = stack_time + timedelta(minutes=5)

            # A real six-input in-camera stack immediately before the bracket.
            for number in range(2495, 2501):
                mtime = stack_time + timedelta(
                    milliseconds=(number - 2495) * 100
                )
                write_media_file(src / f"P629{number}.JPG", mtime)
                write_media_file(src / f"P629{number}.ORF", mtime)
            write_media_file(
                src / "P6292501.JPG", stack_time + timedelta(seconds=1)
            )

            # A long focus bracket.  P6292519 deliberately lacks its RAW to
            # exercise burst safety independently of the all-RAW-backed guard.
            for number in range(2502, 2523):
                mtime = bracket_time + timedelta(
                    milliseconds=(number - 2502) * 100
                )
                write_media_file(src / f"P629{number}.JPG", mtime)
                if number != 2519:
                    write_media_file(src / f"P629{number}.ORF", mtime)

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn(
                "Burst Safety Check: TRIGGERED. Burst continues after output "
                "candidate with 3 frames within 2.0s.",
                result.stdout,
            )
            self.assertIn("Stacked JPG candidates found:  2", result.stdout)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stacked outputs, 12 stack inputs, 41 remaining",
                result.stdout,
            )

            lightroom_files = {p.name for p in files_under(lightroom)}
            stack_files = {p.name for p in files_under(stack_input)}
            self.assertIn("P6292501 stacked.JPG", lightroom_files)
            self.assertIn("P6292519.JPG", lightroom_files)
            self.assertNotIn("P6292519 stacked.JPG", lightroom_files)
            self.assertEqual(
                {
                    name
                    for number in range(2495, 2501)
                    for name in (f"P629{number}.JPG", f"P629{number}.ORF")
                },
                stack_files,
            )
            self.assertEqual(files_under(src), [])

    def test_lightroomimport_immediate_consecutive_stacks_are_not_one_burst(self):
        """Already-claimed frames after an output start the next real stack."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 29, 12, 0, 0)

            # Both complete stacks fit inside the forward guard's two-second
            # window.  Reverse processing accepts the second stack first and
            # its claimed inputs must act as a boundary for the first one.
            for number in (1, 2, 3, 5, 6, 7):
                mtime = base_time + timedelta(milliseconds=number * 100)
                write_media_file(src / f"_629{number:04d}.JPG", mtime)
                write_media_file(src / f"_629{number:04d}.ORF", mtime)
            write_media_file(
                src / "_6290004.JPG",
                base_time + timedelta(milliseconds=400),
            )
            write_media_file(
                src / "_6290008.JPG",
                base_time + timedelta(milliseconds=800),
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(src), "--debug-stacks"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("Burst Safety Check: TRIGGERED", result.stdout)
            self.assertIn("Accepted stacks:               2", result.stdout)
            self.assertIn(
                "Breakdown: 2 stacked outputs, 12 stack inputs, 0 remaining",
                result.stdout,
            )
            self.assertEqual(
                {"_6290004 stacked.JPG", "_6290008 stacked.JPG"},
                {p.name for p in files_under(lightroom)},
            )
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
            self.assertEqual(
                result.stdout.count("Stack detection skipped in "), 1
            )
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
            self.assertIn(
                "Input sequences not all RAW-backed skipped:", result.stdout
            )
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
