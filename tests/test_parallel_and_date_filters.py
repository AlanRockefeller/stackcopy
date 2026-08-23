import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy as stackcopy_module  # noqa: E402

STACKCOPY = ROOT / "stackcopy.py"


def write_media_file(path: Path, mtime: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"{path.name}\n".encode("ascii"))
    timestamp = mtime.timestamp()
    os.utime(path, (timestamp, timestamp))


def files_under(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(item.relative_to(path) for item in path.rglob("*") if item.is_file())


def run_main(args: list[str]) -> tuple[int, str]:
    output = io.StringIO()
    exit_code = 0
    try:
        with (
            mock.patch.object(sys, "argv", [str(STACKCOPY), *args]),
            redirect_stdout(output),
        ):
            stackcopy_module.main()
    except SystemExit as error:
        exit_code = error.code if isinstance(error.code, int) else 1
    return exit_code, output.getvalue()


class ParallelWorkerResultTests(unittest.TestCase):
    def test_parallel_copy_and_stackcopy_count_false_tuple_as_failure(self):
        for mode in ("copy", "stackcopy"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "source"
                destination = root / "destination"
                write_media_file(source / "P8081885.JPG", datetime(2026, 8, 8, 12))

                args = (
                    ["--copy", str(source), str(destination), "--jobs", "2"]
                    if mode == "copy"
                    else ["--stackcopy", str(source), "--jobs", "2"]
                )
                with mock.patch.object(
                    stackcopy_module,
                    "safe_file_operation",
                    return_value=(False, 0),
                ):
                    exit_code, output = run_main(args)

                self.assertEqual(exit_code, 1, output)
                self.assertIn("Failed to process 1 files.", output)
                self.assertIn("0 JPG files", output)

    def test_parallel_copy_and_stackcopy_count_true_tuple_as_success(self):
        for mode in ("copy", "stackcopy"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "source"
                destination = root / "destination"
                write_media_file(source / "P8081885.JPG", datetime(2026, 8, 8, 12))

                args = (
                    ["--copy", str(source), str(destination), "--jobs", "2"]
                    if mode == "copy"
                    else ["--stackcopy", str(source), "--jobs", "2"]
                )
                with mock.patch.object(
                    stackcopy_module,
                    "safe_file_operation",
                    return_value=(True, 1234),
                ):
                    exit_code, output = run_main(args)

                self.assertEqual(exit_code, 0, output)
                self.assertNotIn("Failed to process", output)
                self.assertIn("1 JPG files", output)

    def test_parallel_lightroom_input_moves_unpack_worker_results_and_bytes(self):
        class TrackedBytes:
            def __init__(self, value: int):
                self.value = value
                self.was_added = False

            def __radd__(self, other: int) -> int:
                self.was_added = True
                return other + self.value

        for success in (False, True):
            with self.subTest(success=success), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "source"
                stack_input = root / "StackInput"
                base_time = datetime(2026, 8, 8, 12)
                for number in range(1885, 1888):
                    frame_time = base_time + timedelta(seconds=number - 1885)
                    write_media_file(source / f"P808{number}.JPG", frame_time)
                    write_media_file(source / f"P808{number}.ORF", frame_time)
                write_media_file(
                    source / "P8081888.JPG", base_time + timedelta(seconds=4)
                )

                tracked_bytes = TrackedBytes(100)

                def operation_result(_operation, _src, _dest, operation_name, *_args):
                    if operation_name == "renaming":
                        return True, 0
                    return success, tracked_bytes if success else 0

                with (
                    mock.patch.object(
                        stackcopy_module, "STACK_INPUT_DIR", str(stack_input)
                    ),
                    mock.patch.object(
                        stackcopy_module,
                        "safe_file_operation",
                        side_effect=operation_result,
                    ),
                ):
                    exit_code, output = run_main(
                        ["--lightroom", str(source), "--jobs", "2"]
                    )

                if success:
                    self.assertEqual(exit_code, 0, output)
                    self.assertIn("Moved 6 input files", output)
                    self.assertNotIn("Failed to process", output)
                    self.assertTrue(tracked_bytes.was_added)
                else:
                    self.assertEqual(exit_code, 1, output)
                    self.assertIn("Moved 0 input files", output)
                    self.assertIn("Failed to process 6 files.", output)


class LightroomImportDateFilterTests(unittest.TestCase):
    def run_stackcopy(
        self, args: list[str], lightroom: Path, stack_input: Path
    ) -> subprocess.CompletedProcess[str]:
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

    def create_ordinary_media_for_dates(
        self, source: Path, selected_date: date, other_date: date
    ) -> tuple[set[str], set[str]]:
        selected_time = datetime.combine(selected_date, time(12))
        other_time = datetime.combine(other_date, time(12))
        selected_names = {"P8082001.JPG", "P8082001.ORF", "VID000001.MP4"}
        other_names = {"P8082002.JPG", "P8082002.ORF", "VID000002.MP4"}
        for name in selected_names:
            write_media_file(source / name, selected_time)
        for name in other_names:
            write_media_file(source / name, other_time)
        return selected_names, other_names

    def assert_relative_date_directory(
        self, files: list[Path], expected_date: date, expected_names: set[str]
    ) -> None:
        expected_prefix = Path(str(expected_date.year)) / expected_date.isoformat()
        self.assertEqual(
            {expected_prefix / name for name in expected_names},
            set(files),
        )

    def test_today_filters_ordinary_photo_pairs_and_videos(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date.today()
            selected_names, other_names = self.create_ordinary_media_for_dates(
                source, selected_date, selected_date - timedelta(days=1)
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(source), "--today"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Will move 3 remaining files", result.stdout)
            self.assertIn(
                "Breakdown: 0 stacks (0 stacked outputs, 0 input files), 3 remaining",
                result.stdout,
            )
            self.assert_relative_date_directory(
                files_under(lightroom), selected_date, selected_names
            )
            self.assertEqual({path.name for path in files_under(source)}, other_names)
            self.assertEqual(files_under(stack_input), [])

    def test_yesterday_filters_ordinary_photo_pairs_and_videos(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date.today() - timedelta(days=1)
            selected_names, other_names = self.create_ordinary_media_for_dates(
                source, selected_date, date.today()
            )

            result = self.run_stackcopy(
                ["--lightroomimport", str(source), "--yesterday"],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Will move 3 remaining files", result.stdout)
            self.assert_relative_date_directory(
                files_under(lightroom), selected_date, selected_names
            )
            self.assertEqual({path.name for path in files_under(source)}, other_names)
            self.assertEqual(files_under(stack_input), [])

    def test_explicit_date_filters_all_media_but_keeps_selected_stack_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date(2026, 8, 8)
            other_date = selected_date - timedelta(days=1)
            selected_names, other_names = self.create_ordinary_media_for_dates(
                source, selected_date, other_date
            )

            stack_times = {
                1885: datetime(2026, 8, 7, 23, 59, 59),
                1886: datetime(2026, 8, 8, 0, 0, 1),
                1887: datetime(2026, 8, 8, 0, 0, 3),
            }
            stack_input_names = set()
            for number, frame_time in stack_times.items():
                for extension in ("JPG", "ORF"):
                    name = f"P808{number}.{extension}"
                    stack_input_names.add(name)
                    write_media_file(source / name, frame_time)
            write_media_file(source / "P8081888.JPG", datetime(2026, 8, 8, 0, 0, 5))

            dry_result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(source),
                    "--date",
                    selected_date.isoformat(),
                    "--dry",
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(
                dry_result.returncode, 0, dry_result.stdout + dry_result.stderr
            )
            self.assertIn("Would move 1 stacked output files", dry_result.stdout)
            self.assertIn("Would move 6 stack input files", dry_result.stdout)
            self.assertIn("Would move 3 remaining files", dry_result.stdout)
            self.assertIn("Total planned moves:           10", dry_result.stdout)
            self.assertEqual(
                {path.name for path in files_under(source)},
                selected_names | other_names | stack_input_names | {"P8081888.JPG"},
            )
            self.assertEqual(files_under(lightroom), [])
            self.assertEqual(files_under(stack_input), [])

            result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(source),
                    "--date",
                    selected_date.isoformat(),
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Accepted stacks:               1", result.stdout)
            self.assertIn(
                "Breakdown: 1 stack (1 stacked outputs, 6 input files), 3 remaining",
                result.stdout,
            )
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                selected_names | {"P8081888 stacked.JPG"},
            )
            expected_stack_paths = {
                Path(str(frame_time.year))
                / frame_time.date().isoformat()
                / f"P808{number}.{extension}"
                for number, frame_time in stack_times.items()
                for extension in ("JPG", "ORF")
            }
            self.assertEqual(set(files_under(stack_input)), expected_stack_paths)
            self.assertEqual({path.name for path in files_under(source)}, other_names)

    def test_leave_on_card_with_date_filter_copies_only_selected_media(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date(2026, 8, 8)
            selected_names, other_names = self.create_ordinary_media_for_dates(
                source, selected_date, selected_date - timedelta(days=1)
            )

            result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(source),
                    "--date",
                    selected_date.isoformat(),
                    "--leave-on-card",
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Will copy 3 remaining files", result.stdout)
            self.assertIn("Copied 3 files", result.stdout)
            self.assert_relative_date_directory(
                files_under(lightroom), selected_date, selected_names
            )
            self.assertEqual(
                {path.name for path in files_under(source)},
                selected_names | other_names,
            )
            self.assertEqual(files_under(stack_input), [])

    def test_date_filter_collision_planning_keeps_jpg_raw_pair_at_suffix_two(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date(2026, 8, 8)
            selected_time = datetime.combine(selected_date, time(12))
            other_time = selected_time - timedelta(days=1)

            for extension in ("JPG", "ORF"):
                write_media_file(
                    source / "selected" / f"P8083001.{extension}", selected_time
                )
                write_media_file(
                    source / "outside" / f"P8083001.{extension}", other_time
                )

            destination = (
                lightroom / str(selected_date.year) / selected_date.isoformat()
            )
            destination.mkdir(parents=True)
            (destination / "P8083001.JPG").write_bytes(b"existing jpg")
            (destination / "P8083001.ORF").write_bytes(b"existing raw")

            result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(source),
                    "--date",
                    selected_date.isoformat(),
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("P8083001.JPG -> P8083001__2.JPG", result.stdout)
            self.assertIn("P8083001.ORF -> P8083001__2.ORF", result.stdout)
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                {
                    "P8083001.JPG",
                    "P8083001.ORF",
                    "P8083001__2.JPG",
                    "P8083001__2.ORF",
                },
            )
            self.assertEqual(
                {path.name for path in files_under(source)},
                {"P8083001.JPG", "P8083001.ORF"},
            )

    def test_date_filtered_identical_destination_is_reused_without_suffix(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date(2026, 8, 8)
            selected_time = datetime.combine(selected_date, time(12))
            destination = (
                lightroom / str(selected_date.year) / selected_date.isoformat()
            )

            for extension in ("JPG", "ORF"):
                filename = f"P8084001.{extension}"
                write_media_file(source / filename, selected_time)
                write_media_file(destination / filename, selected_time)

            result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(source),
                    "--date",
                    selected_date.isoformat(),
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("__2", result.stdout)
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                {"P8084001.JPG", "P8084001.ORF"},
            )
            self.assertEqual(files_under(source), [])

    def test_lightroomimport_forces_oldest_first_execution_with_jobs_option(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            selected_date = date(2026, 8, 8)
            base_time = datetime.combine(selected_date, time(12))
            names_by_execution_order = [
                "EARLY_NAME.MP4",
                "MIDDLE_NAME.MP4",
                "LATE_NAME.MP4",
            ]
            for name, offset in (
                ("LATE_NAME.MP4", 2),
                ("MIDDLE_NAME.MP4", 1),
                ("EARLY_NAME.MP4", 0),
            ):
                write_media_file(source / name, base_time + timedelta(seconds=offset))

            result = self.run_stackcopy(
                [
                    "--lightroomimport",
                    str(source),
                    "--date",
                    selected_date.isoformat(),
                    "--jobs",
                    "8",
                    "--verbose",
                ],
                lightroom,
                stack_input,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            execution_positions = [
                result.stdout.index(f"Moved remaining '{name}'")
                for name in names_by_execution_order
            ]
            self.assertEqual(execution_positions, sorted(execution_positions))


if __name__ == "__main__":
    unittest.main()
