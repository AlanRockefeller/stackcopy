"""Parallel ExifTool batches, the plan/run metadata cache, and scan progress."""

import io
import json
import os
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402

PINNED = stackcopy.ExifToolInfo(
    "/usr/bin/exiftool", "13.59", (13, 59), stackcopy.ExifToolSource.PATH
)


def fake_exiftool(tags_by_name, errors=()):
    """A subprocess.run stand-in answering for whichever files it is given."""
    calls = []
    lock = threading.Lock()

    def run(command, **_kwargs):
        paths = [arg for arg in command[1:] if not arg.startswith("-")]
        with lock:
            calls.append(paths)
        payload = []
        for path in paths:
            name = os.path.basename(path)
            if name in errors:
                payload.append({"SourceFile": path, "Error": "File format error"})
            elif name in tags_by_name:
                payload.append(
                    {"SourceFile": path, "StackedImage": tags_by_name[name]}
                )
            else:
                payload.append({"SourceFile": path})
        return SimpleNamespace(
            returncode=1 if errors else 0, stdout=json.dumps(payload)
        )

    return run, calls


class MetadataCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cache = self.root / "cache" / "metadata.json"
        self.paths = []
        for number in range(1, 4):
            path = self.root / "card" / f"P808000{number}.JPG"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"jpeg")
            self.paths.append(str(path))

    def read(self, run, info=PINNED):
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=info),
            mock.patch.object(stackcopy.subprocess, "run", side_effect=run),
            mock.patch.dict(
                os.environ, {stackcopy.METADATA_CACHE_ENV: str(self.cache)}
            ),
            redirect_stdout(io.StringIO()),
        ):
            return stackcopy.read_stacked_image_metadata(self.paths)

    def test_a_second_read_of_an_unchanged_card_does_not_run_exiftool(self):
        run, calls = fake_exiftool({"P8080001.JPG": "9 8", "P8080002.JPG": "0 0"})
        first = self.read(run)
        self.assertEqual(sum(len(batch) for batch in calls), 3)

        calls.clear()
        second = self.read(run)

        self.assertEqual(calls, [])
        self.assertEqual(first, second)
        self.assertEqual(
            second[self.paths[0]].state, stackcopy.StackMetadataState.FOCUS_STACK
        )
        self.assertEqual(second[self.paths[0]].frame_count, 8)
        self.assertEqual(
            second[self.paths[1]].state,
            stackcopy.StackMetadataState.NOT_FOCUS_STACK,
        )
        # A file ExifTool answered for without the tag is remembered as such.
        self.assertEqual(
            second[self.paths[2]].state, stackcopy.StackMetadataState.UNKNOWN
        )

    def test_a_changed_file_is_read_again(self):
        run, calls = fake_exiftool({"P8080001.JPG": "0 0"})
        self.read(run)
        stat_result = os.stat(self.paths[0])
        os.utime(
            self.paths[0],
            ns=(stat_result.st_atime_ns, stat_result.st_mtime_ns + 10**9),
        )

        calls.clear()
        run, calls = fake_exiftool({"P8080001.JPG": "9 5"})
        results = self.read(run)

        self.assertEqual(calls, [[self.paths[0]]])
        self.assertEqual(results[self.paths[0]].frame_count, 5)

    def test_a_different_exiftool_does_not_trust_the_old_answers(self):
        run, calls = fake_exiftool({})
        self.read(run)

        calls.clear()
        upgraded = stackcopy.ExifToolInfo(
            "/usr/bin/exiftool", "13.60", (13, 60), stackcopy.ExifToolSource.PATH
        )
        self.read(run, info=upgraded)

        self.assertEqual(sum(len(batch) for batch in calls), 3)

    def test_files_exiftool_failed_on_are_asked_again(self):
        run, calls = fake_exiftool({}, errors={"P8080002.JPG"})
        self.read(run)

        calls.clear()
        run, calls = fake_exiftool({"P8080002.JPG": "9 4"})
        results = self.read(run)

        self.assertEqual(calls, [[self.paths[1]]])
        self.assertEqual(results[self.paths[1]].frame_count, 4)

    def test_a_damaged_cache_file_is_ignored(self):
        self.cache.parent.mkdir(parents=True)
        self.cache.write_text("{not json", encoding="utf-8")
        run, calls = fake_exiftool({"P8080001.JPG": "9 3"})

        results = self.read(run)

        self.assertEqual(sum(len(batch) for batch in calls), 3)
        self.assertEqual(results[self.paths[0]].frame_count, 3)
        self.assertEqual(json.loads(self.cache.read_text())["version"], 1)

    def test_without_the_environment_variable_nothing_is_cached(self):
        run, calls = fake_exiftool({})
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=PINNED),
            mock.patch.object(stackcopy.subprocess, "run", side_effect=run),
            mock.patch.dict(os.environ, {}, clear=False),
            redirect_stdout(io.StringIO()),
        ):
            os.environ.pop(stackcopy.METADATA_CACHE_ENV, None)
            stackcopy.read_stacked_image_metadata(self.paths)
            stackcopy.read_stacked_image_metadata(self.paths)

        self.assertEqual(sum(len(batch) for batch in calls), 6)
        self.assertFalse(self.cache.exists())


class ParallelBatchTests(unittest.TestCase):
    def test_a_full_card_is_split_across_workers_and_fully_merged(self):
        paths = [f"/card/P80{number:05d}.JPG" for number in range(1, 1001)]
        tags = {os.path.basename(path): "9 6" for path in paths[::2]}
        run, calls = fake_exiftool(tags)
        stderr = io.StringIO()

        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=PINNED),
            mock.patch.object(stackcopy.subprocess, "run", side_effect=run),
            mock.patch.object(stackcopy.os, "cpu_count", return_value=8),
            mock.patch.object(stackcopy, "_PROGRESS_ENABLED", True),
            redirect_stderr(stderr),
        ):
            results = stackcopy.read_stacked_image_metadata(paths)

        # 1000 files over 8 workers, ~4 batches each, 25-250 files per batch.
        self.assertGreater(len(calls), 8)
        # Batches finish in any order, so the one short remainder can be anywhere.
        sizes = sorted(len(batch) for batch in calls)
        self.assertTrue(all(25 <= size <= 250 for size in sizes[1:]))
        self.assertLessEqual(sizes[-1], 250)
        self.assertEqual(sorted(p for batch in calls for p in batch), sorted(paths))
        focus = [
            path
            for path, value in results.items()
            if value.state == stackcopy.StackMetadataState.FOCUS_STACK
        ]
        self.assertEqual(sorted(focus), sorted(paths[::2]))

        progress = [
            line for line in stderr.getvalue().splitlines() if "phase=metadata" in line
        ]
        self.assertEqual(progress[0], "@@SCPROGRESS phase=metadata done=0 total=1000")
        self.assertEqual(
            progress[-1], "@@SCPROGRESS phase=metadata done=1000 total=1000"
        )
        self.assertEqual(len(progress), len(calls) + 1)

    def test_small_batches_are_never_smaller_than_the_floor(self):
        paths = [f"/card/{number}.JPG" for number in range(40)]
        batches = stackcopy._exiftool_batches(paths, workers=8)
        self.assertEqual([len(batch) for batch in batches], [25, 15])

    def test_huge_cards_still_respect_the_batch_ceiling(self):
        paths = [f"/card/{number}.JPG" for number in range(20000)]
        batches = stackcopy._exiftool_batches(paths, workers=8)
        self.assertEqual(max(len(batch) for batch in batches), 250)
        self.assertEqual(sum(len(batch) for batch in batches), 20000)


if __name__ == "__main__":
    unittest.main()
