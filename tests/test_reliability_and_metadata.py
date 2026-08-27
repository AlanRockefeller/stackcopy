import io
import json
import os
import sys
import tempfile
import unittest
from concurrent.futures import wait
from contextlib import ExitStack, redirect_stdout
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402


def write_file(path: Path, content: bytes, when: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    timestamp = when.timestamp()
    os.utime(path, (timestamp, timestamp))


def files_under(path: Path) -> dict[Path, bytes]:
    if not path.exists():
        return {}
    return {
        item.relative_to(path): item.read_bytes()
        for item in path.rglob("*")
        if item.is_file()
    }


def metadata_reader(values_by_name):
    def read(paths):
        return {
            path: values_by_name.get(
                Path(path).name,
                stackcopy.StackMetadata(stackcopy.StackMetadataState.UNKNOWN),
            )
            for path in paths
        }

    return read


def run_main(
    args: list[str],
    *,
    lightroom: Path,
    stack_input: Path,
    metadata=None,
    extra_contexts=(),
    assume_yes=True,
) -> tuple[int, str]:
    output = io.StringIO()
    exit_code = 0
    stackcopy._confirmed_filesystems.clear()
    environment = {
        "STACKCOPY_LIGHTROOM_IMPORT_DIR": str(lightroom),
        "STACKCOPY_ASSUME_YES": "1" if assume_yes else "0",
    }
    with ExitStack() as contexts:
        contexts.enter_context(mock.patch.dict(os.environ, environment, clear=False))
        contexts.enter_context(
            mock.patch.object(stackcopy, "STACK_INPUT_DIR", str(stack_input))
        )
        contexts.enter_context(mock.patch.object(sys, "argv", ["stackcopy.py", *args]))
        if metadata is not None:
            contexts.enter_context(
                mock.patch.object(
                    stackcopy,
                    "read_stacked_image_metadata",
                    side_effect=metadata_reader(metadata),
                )
            )
        for context in extra_contexts:
            contexts.enter_context(context)
        try:
            with redirect_stdout(output):
                stackcopy.main()
        except SystemExit as error:
            exit_code = error.code if isinstance(error.code, int) else 1
    return exit_code, output.getvalue()


UNKNOWN = stackcopy.StackMetadata(stackcopy.StackMetadataState.UNKNOWN)
NO_STACK = stackcopy.StackMetadata(stackcopy.StackMetadataState.NOT_FOCUS_STACK)


class NumericStemAndMetadataParsingTests(unittest.TestCase):
    def test_numeric_suffix_is_the_complete_trailing_run(self):
        cases = {
            "P8081868": ("P", "8081868"),
            "OM_6170005": ("OM_", "6170005"),
            "OM-6170005": ("OM-", "6170005"),
            "_6170005": ("_", "6170005"),
            "camera123456789": ("camera", "123456789"),
            "abc12prefix345678": ("abc12prefix", "345678"),
        }
        for stem, expected in cases.items():
            with self.subTest(stem=stem):
                match = stackcopy.NUMERIC_STEM_REGEX.fullmatch(stem)
                self.assertIsNotNone(match)
                self.assertEqual(match.groups(), expected)

        for stem in (
            "P8081868 stacked",
            "prefix123456suffix",
            "IMG_1234",
            "not-a-camera-name",
            "camera photo123456",
            "camera.photo123456",
            "camera+photo123456",
        ):
            with self.subTest(stem=stem):
                self.assertIsNone(stackcopy.NUMERIC_STEM_REGEX.fullmatch(stem))

    def test_stacked_image_values_are_conservative(self):
        for value, count in (
            ("9 6", 6),
            ("9 15", 15),
            ([9, 20], 20),
            ("Focus-stacked (6 images)", 6),
        ):
            with self.subTest(value=value):
                parsed = stackcopy.parse_stacked_image_value(value)
                self.assertEqual(parsed.state, stackcopy.StackMetadataState.FOCUS_STACK)
                self.assertEqual(parsed.frame_count, count)

        for value in ("0 0", "No", "8 8", [11, 12]):
            with self.subTest(value=value):
                self.assertEqual(
                    stackcopy.parse_stacked_image_value(value).state,
                    stackcopy.StackMetadataState.NOT_FOCUS_STACK,
                )

        for value in (None, "", "9", "Focus-stacked (?)", [9], "99 99"):
            with self.subTest(value=value):
                self.assertEqual(
                    stackcopy.parse_stacked_image_value(value).state,
                    stackcopy.StackMetadataState.UNKNOWN,
                )

    def test_exiftool_json_positive_stack_is_decoded(self):
        path = "/camera/P8217305.JPG"
        payload = [{"SourceFile": path, "StackedImage": "9 15"}]
        with (
            mock.patch.object(
                stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
            ),
            mock.patch.object(
                stackcopy.subprocess,
                "run",
                return_value=SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(payload),
                ),
            ),
        ):
            result = stackcopy.read_stacked_image_metadata([path])[path]

        self.assertEqual(result.state, stackcopy.StackMetadataState.FOCUS_STACK)
        self.assertEqual(result.frame_count, 15)

    def test_exiftool_json_explicit_negative_is_decoded(self):
        path = "/camera/P8217306.JPG"
        payload = [{"SourceFile": path, "Olympus:StackedImage": "0 0"}]
        with (
            mock.patch.object(
                stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
            ),
            mock.patch.object(
                stackcopy.subprocess,
                "run",
                return_value=SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(payload),
                ),
            ),
        ):
            result = stackcopy.read_stacked_image_metadata([path])[path]

        self.assertEqual(result.state, stackcopy.StackMetadataState.NOT_FOCUS_STACK)
        self.assertIsNone(result.frame_count)

    def test_exiftool_batch_maps_relative_and_absolute_source_paths(self):
        camera_dir = ROOT / "test-camera-path"
        positive_absolute = camera_dir / "P8217307.JPG"
        positive_relative = os.path.relpath(positive_absolute, Path.cwd())
        negative_absolute = camera_dir / "P8217308.JPG"
        missing_relative = os.path.relpath(camera_dir / "P8217309.JPG", Path.cwd())
        paths = [positive_relative, str(negative_absolute), missing_relative]
        payload = [
            {
                "SourceFile": str(positive_absolute),
                "StackedImage": "9 6",
            },
            {
                "SourceFile": os.path.relpath(negative_absolute, Path.cwd()),
                "StackedImage": "0 0",
            },
            {"SourceFile": str(camera_dir / "P8217309.JPG")},
        ]
        with (
            mock.patch.object(
                stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
            ),
            mock.patch.object(
                stackcopy.subprocess,
                "run",
                return_value=SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(payload),
                ),
            ),
        ):
            results = stackcopy.read_stacked_image_metadata(paths)

        self.assertEqual(
            results[positive_relative].state,
            stackcopy.StackMetadataState.FOCUS_STACK,
        )
        self.assertEqual(results[positive_relative].frame_count, 6)
        self.assertEqual(
            results[str(negative_absolute)].state,
            stackcopy.StackMetadataState.NOT_FOCUS_STACK,
        )
        self.assertEqual(
            results[missing_relative].state,
            stackcopy.StackMetadataState.UNKNOWN,
        )

    def test_failed_or_undecoded_exiftool_results_stay_unknown(self):
        paths = ["/camera/a.JPG", "/camera/b.JPG"]
        with (
            mock.patch.object(
                stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
            ),
            mock.patch.object(
                stackcopy.subprocess,
                "run",
                return_value=SimpleNamespace(
                    returncode=0,
                    stdout='[{"SourceFile":"/camera/a.JPG"}]',
                ),
            ),
        ):
            results = stackcopy.read_stacked_image_metadata(paths)
        self.assertTrue(
            all(
                value.state == stackcopy.StackMetadataState.UNKNOWN
                for value in results.values()
            )
        )

        degradation = io.StringIO()
        with (
            mock.patch.object(
                stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
            ),
            mock.patch.object(
                stackcopy.subprocess, "run", side_effect=OSError("failed")
            ),
            redirect_stdout(degradation),
        ):
            results = stackcopy.read_stacked_image_metadata(paths)
        self.assertTrue(
            all(
                value.state == stackcopy.StackMetadataState.UNKNOWN
                for value in results.values()
            )
        )
        # A failed invocation says so once and points at the fallback.
        self.assertIn("heuristic stack detection", degradation.getvalue())


class DestinationReliabilityTests(unittest.TestCase):
    def test_same_run_duplicate_names_never_overwrite_with_force(self):
        for force in (False, True):
            with self.subTest(force=force), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                when = datetime(2026, 8, 24, 12)
                for directory, marker in (
                    ("100OMSYS", b"first"),
                    ("101OMSYS", b"second"),
                ):
                    for extension in ("JPG", "ORF"):
                        write_file(
                            source / "DCIM" / directory / f"P8081868.{extension}",
                            marker + extension.encode(),
                            when,
                        )

                args = ["--lightroomimport", str(source)]
                if force:
                    args.append("--force")
                code, output = run_main(
                    args,
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata={},
                )

                self.assertEqual(code, 0, output)
                imported = files_under(lightroom)
                names = {path.name for path in imported}
                self.assertEqual(
                    names,
                    {
                        "P8081868.JPG",
                        "P8081868.ORF",
                        "P8081868__2.JPG",
                        "P8081868__2.ORF",
                    },
                )
                self.assertEqual(len(set(imported.values())), 4)
                self.assertEqual(files_under(source), {})

    def test_case_only_same_run_destinations_get_paired_suffix_even_with_force(self):
        for force in (False, True):
            with self.subTest(force=force), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                when = datetime(2026, 8, 24, 12)
                for directory, basename, marker in (
                    ("100OMSYS", "P8081868", b"upper"),
                    ("101OMSYS", "p8081868", b"lower"),
                ):
                    for extension in ("JPG", "ORF"):
                        write_file(
                            source / "DCIM" / directory / f"{basename}.{extension}",
                            marker + extension.encode(),
                            when,
                        )

                args = ["--lightroomimport", str(source)]
                if force:
                    args.append("--force")
                code, output = run_main(
                    args,
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata={},
                )

                self.assertEqual(code, 0, output)
                imported = files_under(lightroom)
                self.assertEqual(
                    {path.name for path in imported},
                    {
                        "P8081868.JPG",
                        "P8081868.ORF",
                        "p8081868__2.JPG",
                        "p8081868__2.ORF",
                    },
                )
                self.assertEqual(len(imported), 4)
                self.assertEqual(
                    set(imported.values()),
                    {
                        b"upperJPG",
                        b"upperORF",
                        b"lowerJPG",
                        b"lowerORF",
                    },
                )
                self.assertEqual(files_under(source), {})

    def test_identical_reimport_is_noop_with_force_and_uses_no_preflight_space(self):
        for force in (False, True):
            for dry_run in (True, False):
                with (
                    self.subTest(force=force, dry_run=dry_run),
                    tempfile.TemporaryDirectory() as tmp,
                ):
                    root = Path(tmp)
                    source = root / "card"
                    lightroom = root / "Lightroom"
                    stack_input = root / "StackInput"
                    when = datetime(2026, 8, 24, 12)
                    src = source / "VID000001.MP4"
                    dest = lightroom / "2026" / "2026-08-24" / src.name
                    write_file(src, b"same video", when)
                    write_file(dest, b"same video", when)
                    args = ["--lightroomimport", str(source)]
                    if force:
                        args.append("--force")
                    if dry_run:
                        args.append("--dry")

                    with (
                        mock.patch.object(
                            stackcopy,
                            "files_identical",
                            wraps=stackcopy.files_identical,
                        ) as comparator,
                        mock.patch.object(
                            stackcopy, "confirm_if_low_space"
                        ) as preflight,
                    ):
                        code, output = run_main(
                            args,
                            lightroom=lightroom,
                            stack_input=stack_input,
                            metadata={},
                        )

                    self.assertEqual(code, 0, output)
                    self.assertEqual(comparator.call_count, 1)
                    preflight.assert_not_called()
                    self.assertNotIn("Would need --force", output)
                    if dry_run:
                        self.assertIn(
                            "Would delete source because identical destination already exists",
                            output,
                        )
                        self.assertTrue(src.exists())
                    else:
                        self.assertFalse(src.exists())
                    self.assertEqual(dest.read_bytes(), b"same video")

    def test_identical_leave_on_card_copy_is_skipped_with_force(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            src = source / "VID000001.MP4"
            dest = lightroom / "2026" / "2026-08-24" / src.name
            write_file(src, b"same video", when)
            write_file(dest, b"same video", when)
            with (
                mock.patch.object(
                    stackcopy, "files_identical", wraps=stackcopy.files_identical
                ) as comparator,
                mock.patch.object(stackcopy, "confirm_if_low_space") as preflight,
                mock.patch.object(stackcopy, "_atomic_copy2") as atomic_copy,
            ):
                code, output = run_main(
                    [
                        "--lightroomimport",
                        str(source),
                        "--leave-on-card",
                        "--force",
                    ],
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata={},
                )

            self.assertEqual(code, 0, output)
            self.assertEqual(comparator.call_count, 1)
            preflight.assert_not_called()
            atomic_copy.assert_not_called()
            self.assertTrue(src.exists())
            self.assertEqual(dest.read_bytes(), b"same video")

    def test_same_inode_move_remains_a_noop_with_force(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "only-copy.JPG"
            path.write_bytes(b"only copy")
            with mock.patch.object(stackcopy, "_atomic_copy2") as atomic_copy:
                success, bytes_moved = stackcopy.safe_file_operation(
                    "move",
                    str(path),
                    str(path),
                    "moving",
                    force=True,
                )

            self.assertTrue(success)
            self.assertEqual(bytes_moved, 0)
            atomic_copy.assert_not_called()
            self.assertEqual(path.read_bytes(), b"only copy")

    def test_changed_source_invalidates_planned_identical_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.JPG"
            destination = root / "destination.JPG"
            when = datetime(2026, 8, 24, 12)
            write_file(source, b"before", when)
            write_file(destination, b"before", when)
            planned = stackcopy.classify_destination(str(source), str(destination))
            self.assertEqual(planned.state, stackcopy.DestinationState.IDENTICAL)
            write_file(source, b"after!", when + timedelta(seconds=1))

            output = io.StringIO()
            with redirect_stdout(output):
                success, _bytes = stackcopy.safe_file_operation(
                    "move",
                    str(source),
                    str(destination),
                    "moving",
                    dry_run=True,
                    destination_check=planned,
                )
            self.assertFalse(success)
            self.assertIn("Would need --force", output.getvalue())
            self.assertTrue(source.exists())
            self.assertEqual(destination.read_bytes(), b"before")

    def test_zero_byte_destination_is_repaired_without_suffix(self):
        for dry_run in (True, False):
            with self.subTest(dry_run=dry_run), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                when = datetime(2026, 8, 24, 12)
                src = source / "VID000002.MP4"
                dest = lightroom / "2026" / "2026-08-24" / src.name
                write_file(src, b"real video", when)
                write_file(dest, b"", when)
                args = ["--lightroomimport", str(source)]
                if dry_run:
                    args.append("--dry")
                code, output = run_main(
                    args,
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata={},
                )

                self.assertEqual(code, 0, output)
                self.assertIn("exists but is 0 bytes", output)
                self.assertFalse((dest.parent / "VID000002__2.MP4").exists())
                if dry_run:
                    self.assertEqual(dest.read_bytes(), b"")
                    self.assertTrue(src.exists())
                else:
                    self.assertEqual(dest.read_bytes(), b"real video")
                    self.assertFalse(src.exists())

    def test_nonzero_conflict_still_gets_suffix(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "VID000003.MP4", b"new", when)
            dest_dir = lightroom / "2026" / "2026-08-24"
            write_file(dest_dir / "VID000003.MP4", b"old", when)
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )
            self.assertEqual(code, 0, output)
            self.assertEqual((dest_dir / "VID000003.MP4").read_bytes(), b"old")
            self.assertEqual((dest_dir / "VID000003__2.MP4").read_bytes(), b"new")

    def test_force_still_overwrites_a_file_that_predates_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "VID000004.MP4", b"new", when)
            dest_dir = lightroom / "2026" / "2026-08-24"
            write_file(dest_dir / "VID000004.MP4", b"old", when)
            code, output = run_main(
                ["--lightroomimport", str(source), "--force"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )
            self.assertEqual(code, 0, output)
            self.assertEqual((dest_dir / "VID000004.MP4").read_bytes(), b"new")
            self.assertFalse((dest_dir / "VID000004__2.MP4").exists())


class MetadataStackIntegrationTests(unittest.TestCase):
    def make_stack(self, source: Path, count: int, *, missing_first=False):
        base = datetime(2026, 8, 24, 12)
        start = 2 if missing_first else 1
        for number in range(start, count + 1):
            write_file(
                source / f"P808{number:04d}.JPG",
                f"input-{number}".encode(),
                base + timedelta(seconds=number),
            )
        output_name = f"P808{count + 1:04d}.JPG"
        write_file(
            source / output_name,
            b"stack output",
            base + timedelta(seconds=count + 2),
        )
        return output_name, base

    def test_confirmed_6_and_15_frame_stacks_select_exact_inputs(self):
        for count in (6, 15):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                output_name, base = self.make_stack(source, count)
                write_file(source / "VID000010.MP4", b"remaining", base)
                metadata = {
                    output_name: stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, count, f"9 {count}"
                    )
                }

                code, output = run_main(
                    ["--lightroomimport", str(source), "--prefix", "Forest"],
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata=metadata,
                )

                self.assertEqual(code, 0, output)
                self.assertIn("Accepted stacks:               1", output)
                stack_files = files_under(stack_input)
                self.assertEqual(len(stack_files), count)
                self.assertEqual(
                    {path.name for path in stack_files},
                    {f"P808{number:04d}.JPG" for number in range(1, count + 1)},
                )
                lightroom_names = {path.name for path in files_under(lightroom)}
                self.assertIn(
                    f"P808{count + 1:04d} Forest stacked.JPG", lightroom_names
                )
                self.assertIn("VID000010.MP4", lightroom_names)

    def test_metadata_confirmed_stack_can_exceed_heuristic_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            output_name, _base = self.make_stack(source, 20)
            metadata = {
                output_name: stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 20, "9 20"
                )
            }
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
            )
            self.assertEqual(code, 0, output)
            self.assertEqual(len(files_under(stack_input)), 20)
            self.assertNotIn("20-frame input cap", output)

    def test_undatable_confirmed_output_leaves_the_whole_stack_on_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            output_name, _base = self.make_stack(source, 3)
            write_file(
                source / Path(output_name).with_suffix(".ORI"),
                b"output companion",
                datetime(2026, 8, 24, 12),
            )
            metadata = {
                output_name: stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                )
            }
            original_get_file_mtime = stackcopy.get_file_mtime

            def get_file_mtime(record, verbose=False):
                if Path(record["path"]).name == output_name:
                    return None
                return original_get_file_mtime(record, verbose)

            code, output = run_main(
                ["--lightroomimport", str(source), "--date", "2026-08-24"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "get_file_mtime", side_effect=get_file_mtime
                    ),
                ),
            )

            self.assertEqual(code, 0, output)
            self.assertIn("whole stack will be left on the source", output)
            self.assertIn("Accepted stacks:               0", output)
            self.assertEqual(files_under(lightroom), {})
            self.assertEqual(files_under(stack_input), {})
            self.assertEqual(
                {path.name for path in files_under(source)},
                {
                    "P8080001.JPG",
                    "P8080002.JPG",
                    "P8080003.JPG",
                    output_name,
                    Path(output_name).with_suffix(".ORI").name,
                },
            )

    def test_metadata_signal_is_consistent_in_lightroom_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            output_name, _base = self.make_stack(source, 3)
            metadata = {
                output_name: stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                )
            }
            code, output = run_main(
                ["--lightroom", str(source), "--prefix", "Forest", "--jobs", "1"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
            )
            self.assertEqual(code, 0, output)
            self.assertTrue((source / "P8080004 Forest stacked.JPG").exists())
            self.assertEqual(len(files_under(stack_input)), 3)

    def test_incomplete_confirmed_stack_keeps_output_and_available_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            output_name, _base = self.make_stack(source, 6, missing_first=True)
            metadata = {
                output_name: stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 6, "9 6"
                )
            }
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
            )
            self.assertEqual(code, 0, output)
            self.assertIn("1 expected source frame(s) are unavailable", output)
            self.assertEqual(len(files_under(stack_input)), 5)
            self.assertIn(
                "P8080007 stacked.JPG", {p.name for p in files_under(lightroom)}
            )

    def test_metadata_no_blocks_heuristic_and_unknown_falls_back(self):
        for output_metadata, expected_stacks in ((NO_STACK, 0), (UNKNOWN, 1)):
            with (
                self.subTest(state=output_metadata.state),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                base = datetime(2026, 8, 24, 12)
                for number in range(1, 4):
                    for extension in ("JPG", "ORF"):
                        write_file(
                            source / f"P808{number:04d}.{extension}",
                            f"{number}-{extension}".encode(),
                            base + timedelta(seconds=number),
                        )
                output_name = "P8080004.JPG"
                write_file(source / output_name, b"output", base + timedelta(seconds=5))
                code, output = run_main(
                    ["--lightroomimport", str(source)],
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata={output_name: output_metadata},
                )
                self.assertEqual(code, 0, output)
                self.assertIn(
                    f"Accepted stacks:               {expected_stacks}", output
                )
                if expected_stacks:
                    self.assertIn(
                        "P8080004 stacked.JPG", {p.name for p in files_under(lightroom)}
                    )
                else:
                    self.assertEqual(files_under(stack_input), {})
                    self.assertNotIn(
                        "P8080004 stacked.JPG", {p.name for p in files_under(lightroom)}
                    )

    def test_mixed_raw_jpg_heuristic_diagnostic_is_neutral(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            for number in (1, 2):
                for extension in ("JPG", "ORF"):
                    write_file(
                        source / f"P808{number:04d}.{extension}",
                        f"{number}-{extension}".encode(),
                        base + timedelta(seconds=number),
                    )
            write_file(
                source / "P8080003.JPG", b"jpg-only input", base + timedelta(seconds=3)
            )
            write_file(
                source / "P8080004.JPG", b"candidate", base + timedelta(seconds=4)
            )
            code, output = run_main(
                ["--lightroomimport", str(source), "--debug-stacks"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )
            self.assertEqual(code, 0, output)
            self.assertIn("inferred inputs are a mixed set", output)
            self.assertNotIn("Enable RAW+JPG", output)


class RecoveryAndExecutionTests(unittest.TestCase):
    def test_recovery_preserves_stacked_name_and_does_not_double_count(self):
        for recovery_succeeds in (True, False):
            with (
                self.subTest(recovery_succeeds=recovery_succeeds),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root = Path(tmp)
                source = root / "card"
                lightroom = root / "Lightroom"
                stack_input = root / "StackInput"
                output_name, _base = MetadataStackIntegrationTests().make_stack(
                    source, 3
                )
                metadata = {
                    output_name: stackcopy.StackMetadata(
                        stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                    )
                }
                original_operation = stackcopy.safe_file_operation
                output_failures = 0

                def flaky(
                    operation,
                    src,
                    dest,
                    operation_name,
                    force=False,
                    dry_run=False,
                    destination_check=None,
                ):
                    nonlocal output_failures
                    if "stack output" in operation_name:
                        output_failures += 1
                        if not recovery_succeeds or output_failures == 1:
                            return False, 0
                    return original_operation(
                        operation,
                        src,
                        dest,
                        operation_name,
                        force,
                        dry_run,
                        destination_check,
                    )

                code, output = run_main(
                    ["--lightroomimport", str(source), "--prefix", "Forest"],
                    lightroom=lightroom,
                    stack_input=stack_input,
                    metadata=metadata,
                    extra_contexts=(
                        mock.patch.object(
                            stackcopy, "safe_file_operation", side_effect=flaky
                        ),
                    ),
                )
                # Recovery is a degraded outcome: the file is safe, but it is
                # not where the plan said it would go, so the run never reports
                # an ordinary success.
                self.assertEqual(code, 1, output)
                if recovery_succeeds:
                    self.assertIn(
                        "1 primary placement failures; 1 recovered; 0 unrecovered",
                        output,
                    )
                    self.assertIn("Import completed with recovery.", output)
                    self.assertIn("1 file(s) could not be placed as planned", output)
                    self.assertIn(
                        "P8080004 Forest stacked.JPG",
                        {p.name for p in files_under(lightroom)},
                    )
                else:
                    self.assertIn(
                        "1 primary placement failures; 0 recovered; 1 unrecovered",
                        output,
                    )
                    self.assertIn("Failures: 1", output)
                    self.assertNotIn("Failures: 2", output)

    def test_failed_stack_input_is_recovered_without_claiming_primary_placement(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            output_name, _base = MetadataStackIntegrationTests().make_stack(source, 3)
            metadata = {
                output_name: stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                )
            }
            original_operation = stackcopy.safe_file_operation
            failed_input = "P8080002.JPG"
            failed_primary_calls = 0
            recovered_calls = 0

            def flaky(
                operation,
                src,
                dest,
                operation_name,
                force=False,
                dry_run=False,
                destination_check=None,
            ):
                nonlocal failed_primary_calls, recovered_calls
                if Path(src).name == failed_input:
                    if "recovered" not in operation_name:
                        failed_primary_calls += 1
                        return False, 0
                    recovered_calls += 1
                return original_operation(
                    operation,
                    src,
                    dest,
                    operation_name,
                    force,
                    dry_run,
                    destination_check,
                )

            code, output = run_main(
                ["--lightroomimport", str(source), "--prefix", "Forest"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "safe_file_operation", side_effect=flaky
                    ),
                ),
            )

            # Degraded, not failed: the data is safe but was not placed as
            # planned (see test_recovery_preserves_stacked_name...).
            self.assertEqual(code, 1, output)
            self.assertEqual(failed_primary_calls, 1)
            self.assertEqual(recovered_calls, 1)
            self.assertEqual(files_under(source), {})
            self.assertEqual(
                {path.name for path in files_under(stack_input)},
                {"P8080001.JPG", "P8080003.JPG"},
            )
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                {"P8080002.JPG", "P8080004 Forest stacked.JPG"},
            )
            self.assertIn("Done. Imported 4 files", output)
            self.assertIn(
                "Breakdown: 1 stack (1 stacked outputs, 2 input files), "
                "0 remaining, 1 recovered to the Lightroom hierarchy",
                output,
            )
            self.assertNotIn("3 input files", output)
            self.assertIn(
                "1 primary placement failures; 1 recovered; 0 unrecovered",
                output,
            )
            self.assertIn("Failures: 0", output)
            self.assertIn("Import completed with recovery.", output)

    def test_destination_directory_error_keeps_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "VID000020.MP4", b"video", datetime(2026, 8, 24, 12))
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy,
                        "ensure_directory_once",
                        side_effect=PermissionError("denied"),
                    ),
                ),
            )
            self.assertEqual(code, 1, output)
            self.assertIn("Error creating destination directory", output)
            self.assertIn("Done. Imported 0 files", output)
            self.assertIn("Failures: 1", output)

    def test_keyboard_interrupt_prints_partial_summary_and_exits_130(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "VID000021.MP4", b"video", datetime(2026, 8, 24, 12))
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "safe_file_operation", side_effect=KeyboardInterrupt
                    ),
                ),
            )
            self.assertEqual(code, 130, output)
            self.assertIn("Import interrupted by user", output)
            self.assertIn("Completed: 0; failed: 0; remaining: 1", output)
            self.assertNotIn("Done. Imported", output)


class ScanPromptAndTimestampTests(unittest.TestCase):
    def test_ori_counts_as_raw_backing_without_sharing_the_orf_slot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            destination = root / "destination"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            write_file(source / "P8081999.JPG", b"jpg", when)
            write_file(source / "P8081999.ORI", b"ori", when)
            code, output = run_main(
                ["--copy", str(source), str(destination)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )
            self.assertEqual(code, 0, output)
            self.assertEqual(files_under(destination), {})
            self.assertTrue((source / "P8081999.JPG").exists())
            self.assertTrue((source / "P8081999.ORI").exists())

    def test_ori_is_preserved_beside_orf_and_unknown_files_are_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            when = datetime(2026, 8, 24, 12)
            for extension in ("JPG", "ORF", "ORI"):
                write_file(
                    source / f"P8082000.{extension}",
                    extension.encode(),
                    when,
                )
            write_file(source / "note.XMP", b"xmp", when)
            write_file(source / "audio.WAV", b"wav", when)
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )
            self.assertEqual(code, 0, output)
            imported = files_under(lightroom)
            self.assertEqual(
                {path.name for path in imported},
                {"P8082000.JPG", "P8082000.ORF", "P8082000.ORI"},
            )
            self.assertEqual(len(set(imported.values())), 3)
            self.assertIn("Unrecognized files left on source: 2", output)
            self.assertIn("  .WAV: 1", output)
            self.assertIn("  .XMP: 1", output)
            self.assertTrue((source / "note.XMP").exists())
            self.assertTrue((source / "audio.WAV").exists())

    def test_dry_run_shows_plan_before_low_space_and_never_prompts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "VID000030.MP4", b"video", datetime(2026, 8, 24, 12))
            low_space = {
                123: {
                    "bytes": 100,
                    "count": 1,
                    "sample_path": str(lightroom / "file"),
                }
            }
            code, output = run_main(
                ["--lightroomimport", str(source), "--dry", "-i"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy,
                        "estimate_required_bytes_for_ops",
                        return_value=low_space,
                    ),
                    mock.patch.object(
                        stackcopy, "get_existing_parent", return_value=str(root)
                    ),
                    mock.patch.object(
                        stackcopy.shutil,
                        "disk_usage",
                        return_value=SimpleNamespace(total=1000, used=950, free=50),
                    ),
                    mock.patch(
                        "builtins.input", side_effect=AssertionError("prompted")
                    ),
                ),
            )
            self.assertEqual(code, 0, output)
            self.assertLess(
                output.index("DRY RUN: Planned Lightroom import"),
                output.index("DRY RUN WARNING: Low disk space"),
            )
            self.assertNotIn("Proceed anyway?", output)
            self.assertNotIn("Continue?", output)

    def test_real_low_space_confirmation_also_follows_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            write_file(source / "VID000031.MP4", b"video", datetime(2026, 8, 24, 12))
            low_space = {
                123: {
                    "bytes": 100,
                    "count": 1,
                    "sample_path": str(lightroom / "file"),
                }
            }
            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                assume_yes=False,
                extra_contexts=(
                    mock.patch.object(
                        stackcopy,
                        "estimate_required_bytes_for_ops",
                        return_value=low_space,
                    ),
                    mock.patch.object(
                        stackcopy, "get_existing_parent", return_value=str(root)
                    ),
                    mock.patch.object(
                        stackcopy.shutil,
                        "disk_usage",
                        return_value=SimpleNamespace(total=1000, used=950, free=50),
                    ),
                    mock.patch.object(sys.stdin, "isatty", return_value=True),
                    mock.patch("builtins.input", return_value="y"),
                ),
            )
            self.assertEqual(code, 0, output)
            self.assertLess(
                output.index("Planned Lightroom import"),
                output.index("WARNING: Low disk space"),
            )

    def test_out_of_range_timestamp_is_unavailable(self):
        record = {
            "path": "/camera/bad.JPG",
            "entry": mock.Mock(),
            "mtime": None,
            "date": None,
        }
        record["entry"].stat.return_value = SimpleNamespace(st_mtime=10**30)
        fake_datetime = mock.Mock()
        fake_datetime.fromtimestamp.side_effect = OverflowError("out of range")
        output = io.StringIO()
        with (
            mock.patch.object(stackcopy, "datetime", fake_datetime),
            redirect_stdout(output),
        ):
            result = stackcopy.get_file_mtime(record, verbose=True)
        self.assertIsNone(result)
        self.assertIn("Could not determine timestamp", output.getvalue())


def make_heuristic_stack(
    source: Path,
    *,
    input_extensions=("JPG", "ORF"),
    count=3,
    base=None,
) -> tuple[str, datetime]:
    """Write ``count`` input frames plus one JPG-only output frame."""
    base = base or datetime(2026, 8, 24, 12)
    for number in range(1, count + 1):
        for extension in input_extensions:
            write_file(
                source / f"P808{number:04d}.{extension}",
                f"input-{number}-{extension}".encode(),
                base + timedelta(seconds=number),
            )
    output_name = f"P808{count + 1:04d}.JPG"
    write_file(
        source / output_name,
        b"stack output",
        base + timedelta(seconds=count + 2),
    )
    return output_name, base


class OriCompanionSemanticsTests(unittest.TestCase):
    """`.ORI` is a preserved companion, not ordinary RAW backing."""

    def test_helpers_separate_ordinary_raw_from_ori(self):
        ori_only = {"has_raw": False, "has_ori": True}
        raw_only = {"has_raw": True, "has_ori": False}
        both = {"has_raw": True, "has_ori": True}
        neither = {"has_raw": False, "has_ori": False}

        self.assertFalse(stackcopy.has_standard_raw(ori_only))
        self.assertTrue(stackcopy.has_raw_like_companion(ori_only))
        self.assertTrue(stackcopy.has_standard_raw(raw_only))
        self.assertTrue(stackcopy.has_raw_like_companion(raw_only))
        self.assertTrue(stackcopy.has_standard_raw(both))
        self.assertTrue(stackcopy.has_raw_like_companion(both))
        self.assertFalse(stackcopy.has_standard_raw(neither))
        self.assertFalse(stackcopy.has_raw_like_companion(neither))

    def test_ori_only_inputs_do_not_crash_or_satisfy_raw_backing(self):
        # Regression: an ORI-only stem used to set has_raw without a
        # files["raw"] entry, crashing the backward scan with KeyError: 'raw'.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            make_heuristic_stack(source, input_extensions=("JPG", "ORI"))

            code, output = run_main(
                ["--lightroomimport", str(source), "--debug-stacks"],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("Traceback", output)
            self.assertNotIn("KeyError", output)
            self.assertIn("Accepted stacks:               0", output)
            # Nothing was treated as a stack, so nothing lands in StackInput.
            self.assertEqual(files_under(stack_input), {})
            imported = {path.name for path in files_under(lightroom)}
            self.assertEqual(
                imported,
                {
                    "P8080001.JPG",
                    "P8080001.ORI",
                    "P8080002.JPG",
                    "P8080002.ORI",
                    "P8080003.JPG",
                    "P8080003.ORI",
                    "P8080004.JPG",
                },
            )

    def test_jpg_plus_orf_still_counts_as_ordinary_raw_backing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            make_heuristic_stack(source, input_extensions=("JPG", "ORF"))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            self.assertEqual(
                {path.name for path in files_under(stack_input)},
                {
                    f"P808{number:04d}.{extension}"
                    for number in (1, 2, 3)
                    for extension in ("JPG", "ORF")
                },
            )
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                {"P8080004 stacked.JPG"},
            )

    def test_orf_and_ori_are_both_preserved_beside_each_other(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            make_heuristic_stack(source, input_extensions=("JPG", "ORF", "ORI"))

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            stack_files = files_under(stack_input)
            self.assertEqual(
                {path.name for path in stack_files},
                {
                    f"P808{number:04d}.{extension}"
                    for number in (1, 2, 3)
                    for extension in ("JPG", "ORF", "ORI")
                },
            )
            # ORF and ORI keep separate slots: neither overwrote the other.
            self.assertEqual(len(set(stack_files.values())), 9)
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                {"P8080004 stacked.JPG"},
            )

    def test_metadata_confirmed_stack_with_ori_inputs_needs_no_orf(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            output_name, _base = make_heuristic_stack(
                source, input_extensions=("JPG", "ORI")
            )
            metadata = {
                output_name: stackcopy.StackMetadata(
                    stackcopy.StackMetadataState.FOCUS_STACK, 3, "9 3"
                )
            }

            code, output = run_main(
                ["--lightroomimport", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata=metadata,
            )

            self.assertEqual(code, 0, output)
            self.assertIn("Accepted stacks:               1", output)
            self.assertEqual(
                {path.name for path in files_under(stack_input)},
                {
                    f"P808{number:04d}.{extension}"
                    for number in (1, 2, 3)
                    for extension in ("JPG", "ORI")
                },
            )
            self.assertEqual(
                {path.name for path in files_under(lightroom)},
                {"P8080004 stacked.JPG"},
            )

    def test_ori_only_sequence_is_not_a_heuristic_false_positive_in_lightroom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            make_heuristic_stack(source, input_extensions=("JPG", "ORI"))

            code, output = run_main(
                ["--lightroom", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
            )

            self.assertEqual(code, 0, output)
            self.assertNotIn("Traceback", output)
            self.assertEqual(files_under(stack_input), {})
            self.assertEqual(
                {path.name for path in files_under(source)},
                {
                    "P8080001.JPG",
                    "P8080001.ORI",
                    "P8080002.JPG",
                    "P8080002.ORI",
                    "P8080003.JPG",
                    "P8080003.ORI",
                    "P8080004.JPG",
                },
            )


class LegacyLightroomFailureHandlingTests(unittest.TestCase):
    """`--lightroom` matches `--lightroomimport` failure semantics."""

    def make_stack_source(self, source: Path, count=3) -> str:
        return make_heuristic_stack(source, count=count)[0]

    def test_directory_error_is_reported_and_summary_survives(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            self.make_stack_source(source)
            real_ensure = stackcopy.ensure_directory_once

            def ensure(path, cache, dry_run=False):
                # The base stack-input directory is created during startup; only
                # the dated per-stem subdirectory fails here.
                if os.path.abspath(path) != os.path.abspath(str(stack_input)):
                    raise PermissionError("denied")
                return real_ensure(path, cache, dry_run)

            code, output = run_main(
                ["--lightroom", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "ensure_directory_once", side_effect=ensure
                    ),
                ),
            )

            self.assertEqual(code, 1, output)
            self.assertNotIn("Traceback", output)
            self.assertIn("Error creating destination directory", output)
            self.assertIn("for input stem 'P8080003'", output)
            # The summary is still printed and the failures are counted.
            self.assertIn("Done. Processed 1 stacked JPG files", output)
            self.assertIn("Failed to process 6 files.", output)
            # Every input file stayed on the source.
            self.assertEqual(files_under(stack_input), {})
            self.assertEqual(
                {path.name for path in files_under(source)},
                {
                    f"P808{number:04d}.{extension}"
                    for number in (1, 2, 3)
                    for extension in ("JPG", "ORF")
                }
                | {"P8080004 stacked.JPG"},
            )

    def test_keyboard_interrupt_during_output_rename_exits_130(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            base = datetime(2026, 8, 24, 12)
            # Two independent stacks; outputs are renamed newest-first.
            make_heuristic_stack(source, count=3, base=base)
            make_heuristic_stack(
                source,
                count=3,
                base=base.replace(minute=30),
            )
            for number in (5, 6, 7):
                write_file(
                    source / f"P809{number:04d}.JPG",
                    f"second-input-{number}".encode(),
                    base.replace(minute=30) + timedelta(seconds=number),
                )
                write_file(
                    source / f"P809{number:04d}.ORF",
                    f"second-raw-{number}".encode(),
                    base.replace(minute=30) + timedelta(seconds=number),
                )
            write_file(
                source / "P8090008.JPG",
                b"second output",
                base.replace(minute=30) + timedelta(seconds=9),
            )

            real_operation = stackcopy.safe_file_operation
            renames = []

            def flaky(operation, src, dest, description, *rest):
                if description == "renaming":
                    renames.append(src)
                    if len(renames) > 1:
                        raise KeyboardInterrupt
                return real_operation(operation, src, dest, description, *rest)

            code, output = run_main(
                ["--lightroom", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "safe_file_operation", side_effect=flaky
                    ),
                ),
            )

            self.assertEqual(code, 130, output)
            self.assertNotIn("Traceback", output)
            self.assertIn("Interrupted by user", output)
            self.assertIn("Lightroom processing interrupted", output)
            self.assertIn("Re-running Stackcopy is safe.", output)
            self.assertNotIn("Done. Processed", output)
            # The one rename that completed is reported as done; the candidate
            # interrupted mid-rename is reported as unprocessed rather than
            # being folded into the queued-move count.
            self.assertIn("Renamed 1 stacked output(s)", output)
            # The first stack's six queued input moves were never started.
            self.assertIn("queued moves not started: 6", output)
            self.assertIn("1 stacked-output candidate(s) were left unprocessed", output)

            source_names = {path.name for path in files_under(source)}
            # The rename that completed before the interrupt stayed completed...
            self.assertIn("P8090008 stacked.JPG", source_names)
            # ...the second output was never started...
            self.assertIn("P8080004.JPG", source_names)
            self.assertNotIn("P8080004 stacked.JPG", source_names)
            # ...and no input move was started at all.
            self.assertEqual(files_under(stack_input), {})
            for number in (1, 2, 3):
                self.assertIn(f"P808{number:04d}.ORF", source_names)

    def test_moves_completed_before_shutdown_are_not_counted_as_unstarted(self):
        # Ctrl-C can arrive after workers finished their moves but before the
        # main thread consumed those futures.  The partial summary must count
        # the work that really happened.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            self.make_stack_source(source)

            def interrupting_as_completed(futures):
                # Let every queued move finish, then interrupt without ever
                # yielding a future to the consumer loop.
                wait(futures)
                raise KeyboardInterrupt

            code, output = run_main(
                ["--lightroom", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy,
                        "as_completed",
                        side_effect=interrupting_as_completed,
                    ),
                ),
            )

            self.assertEqual(code, 130, output)
            self.assertNotIn("Traceback", output)
            self.assertIn("Lightroom processing interrupted", output)
            # All six input moves really happened, so none are "not started".
            self.assertIn("moved 6 input file(s)", output)
            self.assertIn("queued moves not started: 0", output)
            self.assertEqual(
                {path.name for path in files_under(stack_input)},
                {
                    f"P808{number:04d}.{extension}"
                    for number in (1, 2, 3)
                    for extension in ("JPG", "ORF")
                },
            )
            self.assertEqual(
                {path.name for path in files_under(source)},
                {"P8080004 stacked.JPG"},
            )

    def test_keyboard_interrupt_during_input_moves_exits_130(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "card"
            lightroom = root / "Lightroom"
            stack_input = root / "StackInput"
            self.make_stack_source(source)

            real_operation = stackcopy.safe_file_operation

            def flaky(operation, src, dest, description, *rest):
                if description == "moving input file":
                    raise KeyboardInterrupt
                return real_operation(operation, src, dest, description, *rest)

            code, output = run_main(
                ["--lightroom", str(source)],
                lightroom=lightroom,
                stack_input=stack_input,
                metadata={},
                extra_contexts=(
                    mock.patch.object(
                        stackcopy, "safe_file_operation", side_effect=flaky
                    ),
                ),
            )

            self.assertEqual(code, 130, output)
            self.assertNotIn("Traceback", output)
            self.assertIn("Interrupted by user", output)
            self.assertIn("Lightroom processing interrupted", output)
            self.assertNotIn("Done. Processed", output)
            self.assertEqual(files_under(stack_input), {})
            source_names = {path.name for path in files_under(source)}
            self.assertIn("P8080004 stacked.JPG", source_names)
            for number in (1, 2, 3):
                self.assertIn(f"P808{number:04d}.JPG", source_names)
                self.assertIn(f"P808{number:04d}.ORF", source_names)


class DestinationNameExhaustionTests(unittest.TestCase):
    """Exhausting collision suffixes must fail safely, never overwrite."""

    ATTEMPTS = 3

    def fill_conflicts(self, dest_dir: Path, basename: str) -> dict[Path, bytes]:
        """Occupy every candidate name the search may try."""
        stem, extension = os.path.splitext(basename)
        dest_dir.mkdir(parents=True, exist_ok=True)
        for counter in range(1, self.ATTEMPTS + 1):
            name = basename if counter == 1 else f"{stem}__{counter}{extension}"
            write_file(
                dest_dir / name,
                f"existing-{counter}".encode(),
                datetime(2020, 1, 1, 9),
            )
        return files_under(dest_dir)

    def limit_attempts(self):
        return mock.patch.object(
            stackcopy, "MAX_DESTINATION_NAME_ATTEMPTS", self.ATTEMPTS
        )

    def test_every_candidate_rejected_raises_instead_of_returning_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest_dir = Path(tmp) / "dest"
            source = Path(tmp) / "card"
            self.fill_conflicts(dest_dir, "P8080001.JPG")
            write_file(source / "P8080001.JPG", b"new photo", datetime(2026, 8, 24, 12))
            files_by_type = {
                "jpg": {
                    "basename": "P8080001.JPG",
                    "path": str(source / "P8080001.JPG"),
                }
            }

            with self.limit_attempts():
                with self.assertRaises(stackcopy.DestinationNameExhausted) as caught:
                    stackcopy.pick_unique_basenames_for_stem(
                        str(dest_dir), files_by_type, False, False
                    )

            self.assertIn("P8080001.JPG", str(caught.exception))
            self.assertEqual(caught.exception.dest_dir, str(dest_dir))

    def test_normal_collision_suffixing_is_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest_dir = Path(tmp) / "dest"
            source = Path(tmp) / "card"
            dest_dir.mkdir(parents=True)
            write_file(dest_dir / "P8080001.JPG", b"existing", datetime(2020, 1, 1, 9))
            write_file(dest_dir / "P8080001.ORF", b"existing", datetime(2020, 1, 1, 9))
            for extension in ("JPG", "ORF"):
                write_file(
                    source / f"P8080001.{extension}",
                    f"new-{extension}".encode(),
                    datetime(2026, 8, 24, 12),
                )
            files_by_type = {
                file_type: {
                    "basename": f"P8080001.{extension}",
                    "path": str(source / f"P8080001.{extension}"),
                }
                for file_type, extension in (("jpg", "JPG"), ("raw", "ORF"))
            }

            with self.limit_attempts():
                counter, chosen = stackcopy.pick_unique_basenames_for_stem(
                    str(dest_dir), files_by_type, False, False
                )

            self.assertEqual(counter, 2)
            self.assertEqual(
                chosen, {"jpg": "P8080001__2.JPG", "raw": "P8080001__2.ORF"}
            )

    def _run_exhausted_import(self, extra_args=()):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        when = datetime(2026, 8, 24, 12)
        for extension in ("JPG", "ORF"):
            write_file(source / f"P8080001.{extension}", b"card copy", when)
        dest_dir = lightroom / "2026" / "2026-08-24"
        existing = self.fill_conflicts(dest_dir, "P8080001.JPG")

        code, output = run_main(
            ["--lightroomimport", str(source), *extra_args],
            lightroom=lightroom,
            stack_input=stack_input,
            metadata={},
            extra_contexts=(self.limit_attempts(),),
        )
        return code, output, source, dest_dir, existing

    def test_real_import_fails_safely_and_exits_nonzero(self):
        code, output, source, dest_dir, existing = self._run_exhausted_import()

        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertIn("no collision-free destination name", output)
        self.assertIn(
            "No collision-free destination name could be found for 2 file(s); "
            "they were left on the source.",
            output,
        )
        # No source file was moved or removed.
        self.assertEqual(
            {path.name for path in files_under(source)},
            {"P8080001.JPG", "P8080001.ORF"},
        )
        # No existing destination was overwritten.
        self.assertEqual(files_under(dest_dir), existing)

    def test_dry_run_reports_the_failure_and_does_not_claim_success(self):
        code, output, source, dest_dir, existing = self._run_exhausted_import(
            ["--dry-run"]
        )

        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertIn("DRY RUN complete. 0 files would be moved.", output)
        self.assertIn(
            "No collision-free destination name could be found for 2 file(s); "
            "they would remain on the source.",
            output,
        )
        self.assertEqual(
            {path.name for path in files_under(source)},
            {"P8080001.JPG", "P8080001.ORF"},
        )
        self.assertEqual(files_under(dest_dir), existing)

    def test_legacy_lightroom_exhaustion_is_reported_and_fails(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        make_heuristic_stack(source)
        dest_dir = stack_input / "2026" / "2026-08-24"
        existing = self.fill_conflicts(dest_dir, "P8080001.JPG")

        code, output = run_main(
            ["--lightroom", str(source)],
            lightroom=lightroom,
            stack_input=stack_input,
            metadata={},
            extra_contexts=(self.limit_attempts(),),
        )

        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertIn("cannot move input stem 'P8080001'", output)
        self.assertIn("Done. Processed 1 stacked JPG files", output)
        # The pre-existing destinations are untouched; the other input stems,
        # which have safe names, still moved.
        landed = files_under(dest_dir)
        for name, content in existing.items():
            self.assertEqual(landed[name], content)
        self.assertNotIn(Path("P8080001.ORF"), landed)
        source_names = {path.name for path in files_under(source)}
        self.assertIn("P8080001.JPG", source_names)
        self.assertIn("P8080001.ORF", source_names)

    def test_stacked_output_exhaustion_leaves_the_whole_stack_on_the_source(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        make_heuristic_stack(source)
        dest_dir = lightroom / "2026" / "2026-08-24"
        existing = self.fill_conflicts(dest_dir, "P8080004 stacked.JPG")

        code, output = run_main(
            ["--lightroomimport", str(source)],
            lightroom=lightroom,
            stack_input=stack_input,
            metadata={},
            extra_contexts=(self.limit_attempts(),),
        )

        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertIn("cannot import stacked output 'P8080004.JPG'", output)
        self.assertIn("The whole stack is left on the source", output)
        # Neither the output nor its inputs were moved anywhere.
        self.assertEqual(files_under(stack_input), {})
        self.assertEqual(files_under(dest_dir), existing)
        self.assertEqual(
            {path.name for path in files_under(source)},
            {
                f"P808{number:04d}.{extension}"
                for number in (1, 2, 3)
                for extension in ("JPG", "ORF")
            }
            | {"P8080004.JPG"},
        )

    def test_stack_input_exhaustion_skips_only_that_stem(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        source = root / "card"
        lightroom = root / "Lightroom"
        stack_input = root / "StackInput"
        make_heuristic_stack(source)
        dest_dir = stack_input / "2026" / "2026-08-24"
        existing = self.fill_conflicts(dest_dir, "P8080001.JPG")

        code, output = run_main(
            ["--lightroomimport", str(source)],
            lightroom=lightroom,
            stack_input=stack_input,
            metadata={},
            extra_contexts=(self.limit_attempts(),),
        )

        self.assertEqual(code, 1, output)
        self.assertNotIn("Traceback", output)
        self.assertIn("cannot move stack input 'P8080001'", output)
        landed = files_under(dest_dir)
        for name, content in existing.items():
            self.assertEqual(landed[name], content)
        self.assertNotIn(Path("P8080001.ORF"), landed)
        # The other inputs and the stacked output still went where they belong.
        self.assertIn(Path("P8080002.ORF"), landed)
        self.assertIn(Path("P8080003.ORF"), landed)
        self.assertEqual(
            {path.name for path in files_under(lightroom)},
            {"P8080004 stacked.JPG"},
        )
        # The skipped stem stayed on the source and was not re-planned.
        self.assertEqual(
            {path.name for path in files_under(source)},
            {"P8080001.JPG", "P8080001.ORF"},
        )


if __name__ == "__main__":
    unittest.main()
