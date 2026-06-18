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

    def test_lightroomimport_jpg_only_repro_imports_all_as_remaining(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "card"
            camera_dir = src / "DCIM" / "100OMSYS"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base_time = datetime(2026, 6, 17, 12, 0, 0)

            for i in range(1, 37):
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
            self.assertIn("Will move 36 remaining files", result.stdout)
            self.assertIn(
                "Breakdown: 0 stacked outputs, 0 stack inputs, 36 remaining",
                result.stdout,
            )

            imported = files_under(lightroom)
            self.assertEqual(len(imported), 36)
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

            for i in range(1, 37):
                write_media_file(src / f"_617{i:04d}.JPG", base_time)

            result = self.run_stackcopy(["--lightroom", str(src)], lightroom, stack_input)

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("JPG-only import detected", result.stdout)
            self.assertIn("Done. Processed 0 stacked JPG files", result.stdout)
            self.assertEqual(len(files_under(src)), 36)
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
