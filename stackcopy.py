#!/usr/bin/python3
# SPDX-License-Identifier: MIT

# Stackcopy by Alan Rockefeller (see STACKCOPY_VERSION below)
# 08/24/26

# Copies / renames only the photos that have been stacked in-camera - designed for Olympus / OM System, though it might work for other cameras too.
# Works on Linux, WSL, and Windows.

from __future__ import annotations

import sys
import os
import platform
import shutil
import uuid
import time
import argparse
import re
import errno
import json
import subprocess
from urllib.parse import quote
from bisect import bisect_left
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any

STACKCOPY_VERSION = "1.6.0"

# ---------------------------------------------------------------------------
# Platform helpers
# ---------------------------------------------------------------------------


def _is_wsl() -> bool:
    """Detect if running under Windows Subsystem for Linux."""
    try:
        with open("/proc/version", "r") as f:
            return "microsoft" in f.read().lower()
    except (OSError, IOError):
        return False


IS_WINDOWS = platform.system() == "Windows"
IS_MACOS = platform.system() == "Darwin"
IS_WSL = _is_wsl()

_wsl_warning_shown = False

# ---------------------------------------------------------------------------
# Path comparison for destructive safety checks
# ---------------------------------------------------------------------------
# Stackcopy compares paths in three different ways, and they must not be
# confused with one another:
#
#   * display_path()            - how a path is shown to a human.  Never used
#                                 to decide anything.
#   * safety_comparison_keys()  - equality and containment tests that gate
#                                 destructive work (is this the same file?  is
#                                 the source inside a destination tree?).
#                                 Conservative: it may say "same" when in
#                                 doubt, which only ever keeps a source file.
#   * reservation_key()         - same-run destination reservations.  Folds
#                                 case unconditionally; the cost of a false
#                                 match is a "__2" suffix, so it is always
#                                 taken.
#
# A Windows drive reached through WSL's /mnt/<letter>/ bridge is
# case-insensitive in practice, but Linux's os.path.normcase() is the identity
# function and leaves /mnt/c/Photos and /mnt/c/photos looking like two
# different places.  normcase() alone is therefore never enough here.
_WINDOWS_BACKED_MOUNT_REGEX = re.compile(r"^/mnt/[A-Za-z](?:/|$)")
_WSL_DRIVE_ROOT_REGEX = re.compile(r"^/[A-Za-z](?:/|$)")


def path_comparison_is_case_insensitive(path: str) -> bool:
    """True when two spellings of *path* differing only in case are one file.

    Windows and macOS default to case-insensitive volumes.  On Linux, a path
    under /mnt/<letter>/ is a mounted Windows volume by convention - that is
    exactly what WSL's automount bridge produces - so it is treated as
    case-insensitive too.  The shape test is deliberately *not* gated on
    actually running under WSL, both so the behavior can be exercised
    portably and because treating such a path as case-insensitive can only
    make a safety comparison more cautious.
    """
    if IS_WINDOWS or IS_MACOS:
        return True
    normalized = os.path.abspath(path).replace(os.sep, "/")
    if _WINDOWS_BACKED_MOUNT_REGEX.match(normalized):
        return True
    # Some WSL installs automount the Windows drives at /c, /d, ... instead of
    # under /mnt.  Only trust that shape when actually running under WSL: on a
    # real Linux box "/c" is just a directory.
    return bool(IS_WSL and _WSL_DRIVE_ROOT_REGEX.match(normalized))


def safety_comparison_keys(*paths: str) -> list[str]:
    """Normalize paths so they may be compared with each other for safety.

    Case is folded for *all* of them as soon as *any* one of them lives where
    case does not distinguish files, so a case-only difference can never hide
    a same-file or containment relationship.  Folding a path that really is on
    a case-sensitive filesystem makes Stackcopy more cautious - it can turn a
    move into a no-op, never a deletion.
    """
    normalized = [
        os.path.normcase(os.path.abspath(os.path.normpath(path))) for path in paths
    ]
    if any(path_comparison_is_case_insensitive(path) for path in paths):
        normalized = [key.casefold() for key in normalized]
    return normalized


def _is_wsl_cross_fs(path: str) -> bool:
    """Return True if *path* lives on a Windows volume accessed through WSL's
    /mnt/ bridge (e.g. /mnt/c/..., /mnt/e/...).  These paths go through the
    9P file-system driver and are dramatically slower than native ext4."""
    if not IS_WSL:
        return False
    abspath = os.path.abspath(path)
    return bool(re.match(r"^/mnt/[a-zA-Z]/", abspath))


def _warn_wsl_performance(paths: list[str], operation_desc: str) -> None:
    """Print a one-time warning when WSL cross-filesystem paths are involved."""
    global _wsl_warning_shown
    if _wsl_warning_shown:
        return
    cross = [p for p in paths if _is_wsl_cross_fs(p)]
    if not cross:
        return
    _wsl_warning_shown = True
    prefixes: set[str] = set()
    for p in cross:
        parts = os.path.abspath(p).split("/")
        prefixes.add("/".join(parts[:4]))
    print(
        f"\nPerformance warning: This {operation_desc} operation involves path(s) on a\n"
        f"  Windows filesystem accessed via WSL's /mnt/ bridge, which is significantly\n"
        f"  slower than native Linux filesystems due to 9P protocol overhead."
    )
    for pfx in sorted(prefixes):
        print(f"    {pfx}/...")
    print(
        "\n  Tips to improve speed:\n"
        "    - Copy files to a native Linux path (e.g. ~/photos/) before processing\n"
        "    - Or run stackcopy natively on Windows:  python stackcopy.py ...\n"
        "  See: https://learn.microsoft.com/en-us/windows/wsl/filesystems\n"
    )


# ---------------------------------------------------------------------------
# Optional machine-readable progress (used by the GUI; OFF by default)
# ---------------------------------------------------------------------------
# When STACKCOPY_PROGRESS=1, emit one progress event per line on stderr, which
# is otherwise unused by this program. The GUI parses these to drive a progress
# bar. With the variable unset, nothing is emitted and CLI output is unchanged.

_PROGRESS_ENABLED = os.environ.get("STACKCOPY_PROGRESS") == "1"
_PROGRESS_SENTINEL = "@@SCPROGRESS"
_LOW_SPACE_REPORTS_ENABLED = os.environ.get("STACKCOPY_LOW_SPACE_REPORT") == "1"
_LOW_SPACE_SENTINEL = "@@SCLOWSPACE"


def _emit_progress(file: str | None = None, **fields: Any) -> None:
    """Write a progress event to stderr when STACKCOPY_PROGRESS=1.

    Numeric/token fields are emitted as key=value pairs; the optional *file*
    name is emitted last (after ``file=``) so the reader can treat the rest of
    the line as the name, even though filenames may contain spaces."""
    if not _PROGRESS_ENABLED:
        return
    try:
        parts = " ".join(f"{k}={v}" for k, v in fields.items())
        line = f"{_PROGRESS_SENTINEL} {parts}"
        if file is not None:
            line += f" file={file}"
        sys.stderr.write(line + "\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass


def _emit_low_space_report(report: dict[str, Any]) -> None:
    if not _LOW_SPACE_REPORTS_ENABLED:
        return
    try:
        payload = json.dumps(report, separators=(",", ":"))
        sys.stderr.write(f"{_LOW_SPACE_SENTINEL} {payload}\n")
        sys.stderr.flush()
    except (OSError, TypeError, ValueError):
        pass


def _default_pictures_dir() -> str:
    """Return the user's Pictures directory, respecting platform conventions."""
    if IS_WINDOWS:
        try:
            import ctypes
            import ctypes.wintypes

            CSIDL_MYPICTURES = 0x0027
            buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
            # Fetch the path and check HRESULT (0 == S_OK)
            hresult = ctypes.windll.shell32.SHGetFolderPathW(
                None, CSIDL_MYPICTURES, None, 0, buf
            )
            if hresult == 0:
                if buf.value:
                    return buf.value
        except (ImportError, AttributeError):
            pass
    # On Linux/WSL, prefer ~/pictures if it exists (common convention),
    # otherwise fall back to ~/Pictures.
    home = os.path.expanduser("~")
    lowercase = os.path.join(home, "pictures")
    if os.path.exists(lowercase):
        return lowercase
    return os.path.join(home, "Pictures")


def _lightroom_import_base_dir() -> str:
    """Resolve the --lightroomimport destination (env override or default)."""
    env_base = os.environ.get("STACKCOPY_LIGHTROOM_IMPORT_DIR")
    if env_base:
        return os.path.abspath(os.path.expanduser(env_base))
    return os.path.join(_default_pictures_dir(), "Lightroom")


# ---------------------------------------------------------------------------
# Default paths — override with environment variables if needed:
#   STACKCOPY_STACK_INPUT_DIR       — where stack input photos go
#   STACKCOPY_LIGHTROOM_IMPORT_DIR  — where stacked outputs and remaining files go
# ---------------------------------------------------------------------------

_env_stack_input = os.environ.get("STACKCOPY_STACK_INPUT_DIR")
if _env_stack_input:
    STACK_INPUT_DIR = os.path.abspath(os.path.expanduser(_env_stack_input))
else:
    STACK_INPUT_DIR = os.path.join(
        _default_pictures_dir(), "olympus.stack.input.photos"
    )

# Regex to identify numeric stems for sequence grouping.
# It assumes numeric parts are 6 or more digits, common for Olympus/OM System in-camera stacking.
# Stems with fewer digits (e.g., 4-digit counters) will not be treated as numeric sequences.

NUMERIC_STEM_REGEX = re.compile(r"^([A-Za-z0-9_-]*?)(\d{6,})$")
CAMERA_ROLL_DIR_REGEX = re.compile(r"^(\d{3})([A-Za-z0-9_]{5})$")

# Stack-detection timing/burst thresholds (shared by the --lightroomimport and
# --lightroom backward-scan passes).
MAX_OUTPUT_LAG_SECONDS = 120
MAX_INPUT_GAP_SECONDS = 6
MAX_BURST_GAP_SECONDS = 2.0
MIN_STACK_INPUT_FRAMES = 3
MAX_STACK_INPUT_FRAMES = 15
BURST_EXTRA_FRAMES_REQUIRED = 3
# Secondary sanity boundary for frames borrowed from the *previous* camera
# folder.  Folder provenance is the principal protection (see
# get_stack_sequence); this only rejects candidates whose timestamps make a
# shared capture implausible.  Deliberately generous: a real in-camera stack
# captures every frame within seconds, while an unrelated older photo that
# happens to sit at the missing number is typically hours or days away.
MAX_CROSS_FOLDER_INPUT_GAP_SECONDS = 300

# How many "__N" collision suffixes to try before giving up on a stem.  Kept as
# a module-level constant so the exhaustion path can be exercised cheaply.
MAX_DESTINATION_NAME_ATTEMPTS = 999


def collect_consecutive_probe_stems(
    sequence,
    *,
    start_index,
    expected_num,
    direction,
    required_count,
) -> tuple[str, ...]:
    """Collect up to ``required_count`` numerically consecutive probe stems.

    ``direction`` is ``-1`` for a backward probe and ``+1`` for a forward
    probe. Collection stops at sequence bounds or the first numeric gap.
    """
    if direction not in (-1, 1):
        raise ValueError("direction must be -1 or +1")
    if required_count < 0:
        raise ValueError("required_count must be non-negative")

    stems = []
    probe_index = start_index
    probe_expected_num = expected_num
    while 0 <= probe_index < len(sequence) and len(stems) < required_count:
        probe_num, probe_stem = sequence[probe_index]
        if probe_num != probe_expected_num:
            break
        stems.append(probe_stem)
        probe_expected_num += direction
        probe_index += direction

    return tuple(stems)


@dataclass
class PlannedMove:
    """A single file move operation planned during --lightroomimport."""

    src_path: str
    dest_path: str  # final destination (single move, no intermediate steps)
    category: str  # "stack_output", "stack_input", "remaining"
    stem: str
    file_type: str  # "jpg", "raw", "ori", or "video"
    mtime: datetime | None
    basename_orig: str  # original filename
    basename_dest: (
        str  # destination filename (may include "stacked" rename + collision suffix)
    )
    dest_dir: str
    stack_output_name: str | None = None
    destination_check: DestinationCheck | None = None


class DestinationState(Enum):
    """Relevant state of a destination at planning/execution time."""

    ABSENT = "absent"
    IDENTICAL = "identical"
    ZERO_BYTE_RECOVERABLE = "zero_byte_recoverable"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class FileFingerprint:
    device: int
    inode: int
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class DestinationCheck:
    state: DestinationState
    source_fingerprint: FileFingerprint | None
    destination_fingerprint: FileFingerprint | None


class OperationOutcome(Enum):
    """How a single copy/move actually ended.

    ``COPIED_SOURCE_REMAINS`` exists because a cross-device move is two steps:
    the destination can be written durably and the source deletion can still
    fail.  That is neither a normal move nor a failure, and conflating it with
    either misreports what is on the card.
    """

    SUCCESS = "success"
    COPIED_SOURCE_REMAINS = "copied_source_remains"
    FAILED = "failed"


@dataclass(frozen=True)
class OperationResult:
    """Result of one file operation.

    Truthy whenever the destination was placed, so the many existing
    ``success, _ = safe_file_operation(...)`` call sites keep their meaning
    ("the destination is there, do not retry or recover it").
    """

    outcome: OperationOutcome
    forced_overwrite: bool = False

    def __bool__(self) -> bool:
        return self.outcome is not OperationOutcome.FAILED


def operation_outcome_of(result) -> OperationOutcome:
    """Read the outcome from a result, tolerating a plain boolean.

    Tests (and any future caller) may substitute a bare ``(bool, int)`` tuple
    for ``safe_file_operation``; those degrade to SUCCESS/FAILED.
    """
    if isinstance(result, OperationResult):
        return result.outcome
    return OperationOutcome.SUCCESS if result else OperationOutcome.FAILED


def forced_overwrite_of(result) -> bool:
    """True when this result overwrote a differing pre-existing destination."""
    return isinstance(result, OperationResult) and result.forced_overwrite


class StackMetadataState(Enum):
    UNKNOWN = "unknown"
    FOCUS_STACK = "focus_stack"
    NOT_FOCUS_STACK = "not_focus_stack"


@dataclass(frozen=True)
class StackMetadata:
    state: StackMetadataState
    frame_count: int | None = None
    raw_value: str | None = None


def has_standard_raw(record) -> bool:
    """True when a stem has ordinary RAW backing (ORF/CR2/NEF/ARW/...).

    ``.ORI`` deliberately does not count here.  It is an Olympus/OM companion
    to a High Res Shot or in-camera-processed JPG, not evidence that the frame
    was captured as a normal RAW-backed exposure, so the focus-stack input
    heuristic must not treat it as ordinary RAW backing.
    """
    return bool(record.get("has_raw"))


def has_raw_like_companion(record) -> bool:
    """True when a stem has ordinary RAW backing *or* an ``.ORI`` companion.

    Used where the question is "does this JPG have a sibling original file?"
    (for example, deciding whether a JPG is an unpaired in-camera output)
    rather than "was this frame a normal RAW capture?".
    """
    return bool(record.get("has_raw") or record.get("has_ori"))


def get_file_mtime(file_record, verbose=False):
    """Lazily fetch mtime, caching the result."""
    if file_record.get("mtime") is not None:
        return file_record["mtime"]
    if "entry" in file_record:
        try:
            stat_info = file_record["entry"].stat(follow_symlinks=False)
            mtime_dt = datetime.fromtimestamp(stat_info.st_mtime)
            file_record["mtime"] = mtime_dt
            file_record["date"] = mtime_dt.date()
            return mtime_dt
        except (OSError, ValueError, OverflowError) as e:
            if verbose:
                print(
                    f"Warning: Could not determine timestamp for "
                    f"'{file_record['path']}': {e}"
                )
            pass
    return None


def get_file_date(file_record, verbose=False):
    """Lazily fetch date, caching the result."""
    if file_record.get("date") is not None:
        return file_record["date"]
    # Calling get_file_mtime will populate both mtime and date
    if get_file_mtime(file_record, verbose):
        return file_record.get("date")
    return None


def path_is_within(path: str, root: str) -> bool:
    """True if `path` equals `root` or is nested within it (both absolute).

    Both sides go through safety_comparison_keys(), so a case-only difference
    cannot hide a containment relationship on Windows, on macOS, or on a
    Windows volume seen through WSL's /mnt/ bridge.
    """
    normalized_path, normalized_root = safety_comparison_keys(path, root)
    try:
        return os.path.commonpath([normalized_path, normalized_root]) == normalized_root
    except ValueError:
        # Different drives (Windows) or an abs/relative mismatch -> not within.
        return False


_CARD_HOUSEKEEPING_DIRS = {
    ".spotlight-v100",
    ".trashes",
    "lost.dir",
    "misc",
    "private",
    "system volume information",
}
_CARD_HOUSEKEEPING_FILES = {
    ".ds_store",
    "desktop.ini",
    "thumbs.db",
}


def card_would_be_empty_after(source_dir: str, removed_paths=()) -> bool:
    """Return whether only ignorable camera/card housekeeping would remain.

    Directory shells such as ``DCIM`` do not make a card non-empty. Files in
    the common camera/card housekeeping directories above are ignored, as are
    a few operating-system metadata files. Every other file must be in
    ``removed_paths`` for this to return true.
    """
    removed = {
        os.path.normcase(os.path.realpath(os.fspath(path))).casefold()
        for path in removed_paths
    }
    try:
        for root, dirs, files in os.walk(source_dir):
            relative_root = os.path.relpath(root, source_dir)
            parts = () if relative_root == "." else _path_parts(relative_root)
            if any(part.casefold() in _CARD_HOUSEKEEPING_DIRS for part in parts):
                dirs[:] = []
                continue
            dirs[:] = [
                name
                for name in dirs
                if name.casefold() not in _CARD_HOUSEKEEPING_DIRS
            ]
            for filename in files:
                if filename.casefold() in _CARD_HOUSEKEEPING_FILES:
                    continue
                path_key = os.path.normcase(
                    os.path.realpath(os.path.join(root, filename))
                ).casefold()
                if path_key not in removed:
                    return False
    except OSError:
        return False
    return True


def _path_parts(path: str) -> tuple[str, ...]:
    """Split a relative path using the current platform's path rules."""
    normalized = os.path.normpath(path)
    parts: list[str] = []
    while normalized not in ("", "."):
        normalized, tail = os.path.split(normalized)
        if not tail:
            break
        parts.append(tail)
    return tuple(reversed(parts))


def source_is_removable(source_dir: str) -> bool:
    """Best-effort removable-media detection for the plan payload."""
    absolute = os.path.abspath(source_dir)
    if IS_WINDOWS:
        try:
            import ctypes

            drive, _ = os.path.splitdrive(absolute)
            root = drive + "\\" if drive else absolute
            return ctypes.windll.kernel32.GetDriveTypeW(root) == 2
        except (AttributeError, OSError):
            return False
    if IS_MACOS:
        return path_is_within(absolute, "/Volumes")
    normalized = absolute.replace(os.sep, "/")
    if _WINDOWS_BACKED_MOUNT_REGEX.match(normalized) or (
        IS_WSL and _WSL_DRIVE_ROOT_REGEX.match(normalized)
    ):
        # A Windows volume reached through WSL's drive bridge.  /mnt/e may be
        # a card reader but /mnt/c and /mnt/d are the machine's fixed disks,
        # and nothing on this side of the bridge can tell them apart.  Say no:
        # a missed "your card is empty" hint costs nothing, while a wrong one
        # tells somebody to format their internal drive in the camera.
        return False
    return normalized.startswith(("/media/", "/run/media/", "/mnt/"))


def lightroom_import_source_conflict(
    src_dir: str, lightroom_base_dir: str, stack_input_dir: str
) -> str | None:
    """Return an error message if a --lightroomimport source sits in a destination.

    Returns None when the layout is fine.
    """
    real_source = os.path.realpath(src_dir)
    for label, destination_root in (
        ("Lightroom import destination", lightroom_base_dir),
        ("stack input destination", stack_input_dir),
    ):
        real_root = os.path.realpath(destination_root)
        if not path_is_within(real_source, real_root):
            continue
        # Folded equality, so a case-only difference on a Windows/WSL volume
        # reads as "is" rather than the nonsensical "is inside".
        source_key, root_key = safety_comparison_keys(real_source, real_root)
        relation = "is" if source_key == root_key else "is inside"
        return (
            f"Error: the source folder {relation} the {label}.\n"
            f"  Source:      {display_path(real_source)}\n"
            f"  Destination: {display_path(real_root)}\n"
            "  Importing a destination back into itself would re-sort and rename "
            "files\n  Stackcopy has already filed. Point --lightroomimport at the "
            "camera card\n  or another folder outside the destination trees."
        )
    return None


def _relative_dir_lookup_key(relative_dir: str) -> str:
    """Normalize a scanned relative directory for cross-platform lookups."""
    return os.path.normcase(os.path.normpath(relative_dir or ".")).casefold()


def previous_adjacent_camera_dir(
    relative_dir: str, known_relative_dirs_by_key: dict[str, str]
) -> str | None:
    """Return the previous sibling camera roll dir, if `relative_dir` has one."""
    normalized = os.path.normpath(relative_dir or ".")
    if normalized == ".":
        return None

    parent, folder = os.path.split(normalized)
    match = CAMERA_ROLL_DIR_REGEX.fullmatch(folder)
    if not match:
        return None

    folder_number = int(match.group(1))
    if folder_number <= 0:
        return None

    suffix = match.group(2)
    previous_folder = f"{folder_number - 1:03d}{suffix}"
    previous_dir = os.path.normpath(os.path.join(parent, previous_folder))
    return known_relative_dirs_by_key.get(_relative_dir_lookup_key(previous_dir))


def iter_source_file_entries(src_dir: str, recursive: bool = False, exclude_dirs=()):
    """Yield files from src_dir, optionally descending into subdirectories.

    Subdirectories whose real path is at or under any path in `exclude_dirs`
    are skipped, so a recursive scan never descends into its own destination
    dirs. Unreadable subdirectories are reported and skipped rather than
    aborting the whole scan (a failure on the top-level src_dir still
    propagates to the caller).
    """
    with os.scandir(src_dir) as entries:
        sorted_entries = sorted(entries, key=lambda e: e.name.lower())

    for entry in sorted_entries:
        if entry.is_file():
            yield entry
        elif recursive and entry.is_dir(follow_symlinks=False):
            if any(
                path_is_within(os.path.realpath(entry.path), excl)
                for excl in exclude_dirs
            ):
                continue
            try:
                yield from iter_source_file_entries(
                    entry.path, recursive=True, exclude_dirs=exclude_dirs
                )
            except OSError as scan_error:
                print(
                    f"Warning: skipping unreadable directory '{entry.path}': {scan_error}"
                )


def get_stem_mtime(record, verbose=False):
    """
    Get the mtime for a stem record, preferring RAW over JPG.
    Utilizes get_file_mtime to ensure caching and logging.
    """
    raw_files = record["files"].get("raw")
    if raw_files:
        mtime = get_file_mtime(raw_files, verbose)
        if mtime:
            return mtime

    ori_file = record["files"].get("ori")
    if ori_file:
        mtime = get_file_mtime(ori_file, verbose)
        if mtime:
            return mtime

    jpg_files = record["files"].get("jpg")
    if jpg_files:
        mtime = get_file_mtime(jpg_files, verbose)
        if mtime:
            return mtime

    return None


def prefix_validation_error(prefix: str | None) -> str | None:
    """Return an error message for an unusable ``--prefix``, or None.

    ``--prefix`` becomes part of a destination filename, so it must not be
    able to redirect that filename into another directory or make it
    impossible to create.  Deliberately conservative: anything questionable is
    rejected with an explanation instead of being silently rewritten, and
    ordinary human-readable prefixes (spaces, punctuation, accents) still pass.
    """
    if prefix is None:
        return None
    separators = {"/", "\\", os.sep}
    if os.altsep:
        separators.add(os.altsep)
    if any(separator in prefix for separator in separators):
        return "--prefix may not contain path separators."
    if "\0" in prefix:
        return "--prefix may not contain NUL characters."
    if any(ord(character) < 32 or ord(character) == 127 for character in prefix):
        return "--prefix may not contain control characters."
    if prefix.strip() in {".", ".."}:
        return "--prefix may not be '.' or '..'."
    if IS_WINDOWS:
        reserved = ':*?"<>|'
        if any(character in prefix for character in reserved):
            return (
                "--prefix may not contain any of " + " ".join(reserved) + " on Windows."
            )
    return None


def create_new_filename(stem, ext, prefix=None):
    """Create a new filename with optional prefix and 'stacked' suffix."""
    parts = [stem]
    if prefix:
        # Trim whitespace from prefix to avoid double spaces
        prefix = prefix.strip()
        if prefix:  # Only add if not empty after stripping
            parts.append(prefix)
    parts.append("stacked")
    # Join with single space and collapse any multiple spaces
    new_stem = " ".join(parts)
    # Collapse multiple spaces into single spaces
    new_stem = re.sub(r"\s+", " ", new_stem)
    return f"{new_stem}{ext}"


def ensure_directory_once(path, created_cache, dry_run=False):
    """Create a directory only once per execution (no-op during dry runs)."""
    if dry_run:
        return

    norm_path = os.path.abspath(os.path.normpath(path))
    if norm_path in created_cache:
        return
    os.makedirs(norm_path, exist_ok=True)
    created_cache.add(norm_path)


def is_already_processed(filename):
    """Check if a file has already been processed (contains 'stacked' as a word)."""
    stem, _ext = os.path.splitext(filename)
    # Use word boundary regex to match 'stacked' as a complete word
    # This will match: "image stacked.jpg", "stacked_image.jpg", "stacked-photo.jpg", etc.
    return bool(re.search(r"\bstacked\b", stem.lower()))


def normalize_path(path):
    """Normalize and resolve a path to its absolute form."""
    return os.path.abspath(os.path.expanduser(path))


def display_path(path):
    """Format a path for user display, shortening to ~ on non-Windows platforms."""
    if os.name == "nt":
        return os.path.abspath(path)
    home = os.path.expanduser("~")
    abspath = os.path.abspath(path)
    if abspath == home:
        return "~"
    prefix = home + os.sep
    if abspath.startswith(prefix):
        return "~" + os.sep + abspath[len(prefix) :]
    return abspath


def paths_are_same(path1, path2):
    """Check if two paths refer to the same location, handling non-existent paths."""
    norm_path1 = normalize_path(path1)
    norm_path2 = normalize_path(path2)

    # If both paths exist, use samefile
    if os.path.exists(norm_path1) and os.path.exists(norm_path2):
        try:
            return os.path.samefile(norm_path1, norm_path2)
        except OSError:
            return False

    # Otherwise, compare normalized paths
    return norm_path1 == norm_path2


def files_identical(src_path, dest_path, chunk_size=1024 * 1024):
    """Return True if src and dest have identical content, False otherwise."""
    try:
        if os.path.getsize(src_path) != os.path.getsize(dest_path):
            return False
        with open(src_path, "rb") as fsrc, open(dest_path, "rb") as fdst:
            while True:
                b1 = fsrc.read(chunk_size)
                b2 = fdst.read(chunk_size)
                if not b1 and not b2:
                    return True
                if b1 != b2:
                    return False
    except OSError:
        # If we can't read either file, treat them as non-identical and let the caller decide.
        return False


def inode_identity_is_meaningful(
    first: FileFingerprint | None, second: FileFingerprint | None
) -> bool:
    """True when ``(device, inode)`` may be trusted to prove file identity.

    Some filesystems (notably several FUSE and network mounts, and some
    Windows shares seen through WSL) report ``st_ino == 0`` for every file.
    Two unrelated files would then look like the same inode on the same
    device, so the shortcut must not be taken; callers fall back to ordinary
    size/content comparison instead.
    """
    if first is None or second is None:
        return False
    return first.inode != 0 and second.inode != 0


def paths_resolve_to_same_file(first: str, second: str) -> bool:
    """Path-based same-file test, used where inode identity is untrustworthy.

    ``os.path.samefile`` is itself ``(st_dev, st_ino)`` based, so it cannot
    answer this on a filesystem that reports inode zero.  The comparison is
    deliberately case-folding where case cannot distinguish files (see
    safety_comparison_keys): under WSL, ``/mnt/c/Photos/P8081234.JPG`` and
    ``/mnt/c/photos/p8081234.jpg`` are one file, and concluding otherwise
    would let a "move" delete the only copy of a photo.
    """
    try:
        first_key, second_key = safety_comparison_keys(
            os.path.realpath(first), os.path.realpath(second)
        )
    except OSError:
        return False
    return first_key == second_key


def is_same_physical_file(
    src_path: str, dest_path: str, check: DestinationCheck | None = None
) -> bool:
    """True when src and dest are the very same file on disk.

    A move must never delete the source in that case: it would delete the
    destination as well.
    """
    if check is not None and inode_identity_is_meaningful(
        check.source_fingerprint, check.destination_fingerprint
    ):
        assert check.source_fingerprint is not None
        assert check.destination_fingerprint is not None
        if (
            check.source_fingerprint.device == check.destination_fingerprint.device
            and check.source_fingerprint.inode == check.destination_fingerprint.inode
        ):
            return True
    return paths_resolve_to_same_file(src_path, dest_path)


def _file_fingerprint(path: str) -> FileFingerprint | None:
    """Return a cheap identity/change fingerprint, or None when unavailable."""
    try:
        stat_result = os.stat(path, follow_symlinks=False)
    except OSError:
        return None
    return FileFingerprint(
        device=stat_result.st_dev,
        inode=stat_result.st_ino,
        size=stat_result.st_size,
        mtime_ns=stat_result.st_mtime_ns,
    )


def classify_destination(src_path: str, dest_path: str) -> DestinationCheck:
    """Classify a destination without applying overwrite policy.

    This is shared by planning and execution.  A planning result may safely be
    reused only while both cheap stat fingerprints remain unchanged.
    """
    source_fingerprint = _file_fingerprint(src_path)
    destination_fingerprint = _file_fingerprint(dest_path)
    if destination_fingerprint is None:
        return DestinationCheck(
            DestinationState.ABSENT, source_fingerprint, destination_fingerprint
        )

    if source_fingerprint is not None:
        if inode_identity_is_meaningful(
            source_fingerprint, destination_fingerprint
        ) and (
            source_fingerprint.device == destination_fingerprint.device
            and source_fingerprint.inode == destination_fingerprint.inode
        ):
            return DestinationCheck(
                DestinationState.IDENTICAL,
                source_fingerprint,
                destination_fingerprint,
            )
        if source_fingerprint.size > 0 and destination_fingerprint.size == 0:
            return DestinationCheck(
                DestinationState.ZERO_BYTE_RECOVERABLE,
                source_fingerprint,
                destination_fingerprint,
            )
        if source_fingerprint.size == destination_fingerprint.size and files_identical(
            src_path, dest_path
        ):
            return DestinationCheck(
                DestinationState.IDENTICAL,
                source_fingerprint,
                destination_fingerprint,
            )

    return DestinationCheck(
        DestinationState.CONFLICT, source_fingerprint, destination_fingerprint
    )


def refresh_destination_check(
    src_path: str,
    dest_path: str,
    planned_check: DestinationCheck | None,
) -> DestinationCheck:
    """Reuse a planned classification only if neither path has changed."""
    if planned_check is not None:
        source_now = _file_fingerprint(src_path)
        destination_now = _file_fingerprint(dest_path)
        if (
            source_now == planned_check.source_fingerprint
            and destination_now == planned_check.destination_fingerprint
        ):
            return planned_check
    return classify_destination(src_path, dest_path)


class DestinationNameExhausted(Exception):
    """No collision-free destination basename could be found for a stem.

    Raised instead of returning a name that already failed the collision or
    same-run reservation checks: Stackcopy must never knowingly plan a move
    onto a destination it just rejected.
    """

    def __init__(self, dest_dir: str, basenames):
        self.dest_dir = dest_dir
        self.basenames = sorted(basenames)
        super().__init__(
            f"no collision-free destination name in '{dest_dir}' after "
            f"{MAX_DESTINATION_NAME_ATTEMPTS} attempts for: "
            + ", ".join(self.basenames)
        )


def add_counter_suffix(basename: str, counter: int) -> str:
    """
    Insert a counter suffix before the extension: IMG.JPG -> IMG__2.JPG
    Counter=1 returns the original basename.
    """
    if counter <= 1:
        return basename
    stem, ext = os.path.splitext(basename)
    return f"{stem}__{counter}{ext}"


def reservation_key(path: str) -> str:
    """Return a conservative key for same-run destination reservations.

    Unlike safety_comparison_keys(), this folds case unconditionally rather
    than only where case cannot distinguish files: the worst a false match can
    do here is add an unnecessary ``__2`` suffix, which is far preferable to
    letting two files from one run target the same effective destination.
    """
    normalized = os.path.normcase(os.path.abspath(os.path.normpath(path)))
    return normalized.casefold()


def dest_conflicts(
    src_path: str,
    dest_path: str,
    force: bool,
    destination_checks: dict[tuple[str, str], DestinationCheck] | None = None,
) -> bool:
    """
    Return True if dest_path exists and we should NOT overwrite it.
    If contents are identical, it's not a conflict (we treat it as safe).
    If --force is set, we consider it not a conflict (user explicitly wants overwrite).

    A matching camera basename does not prove that two files are the same photo.
    For example, an OM-1 using File Name = Reset can reuse a sequence number after
    cards are swapped or removed, producing two different P8081868.ORF files.
    """
    check = classify_destination(src_path, dest_path)
    if destination_checks is not None:
        destination_checks[(src_path, dest_path)] = check
    if check.state in {
        DestinationState.ABSENT,
        DestinationState.IDENTICAL,
        DestinationState.ZERO_BYTE_RECOVERABLE,
    }:
        return False
    return not force


def pick_unique_basenames_for_stem(
    dest_dir: str,
    files_by_type: dict[str, Any],
    force: bool,
    _dry_run: bool,
    reserved_paths: set[str] | None = None,
    destination_checks: dict[tuple[str, str], DestinationCheck] | None = None,
) -> tuple[int, dict[str, str]]:
    """
    Choose a single counter for *all* files in this stem (e.g., JPG+ORF) so they stay paired.
    Returns (counter, {file_type: chosen_basename}).
    Raises DestinationNameExhausted if no collision-free set of names exists.
    Existing files may be overwritten with --force, but destinations reserved by
    this invocation always receive a counter suffix.

    reserved_paths: optional set of destination paths already claimed by earlier planned
    moves (used by --lightroomimport plan-then-execute to avoid two planned moves
    targeting the same path).
    """
    orig = {ft: fi["basename"] for ft, fi in files_by_type.items() if fi}
    srcs = {ft: fi["path"] for ft, fi in files_by_type.items() if fi}

    # Suffixes protect against genuine camera filename reuse, not just importing an
    # unrelated directory twice. In particular, OM-1 File Name = Reset can reuse
    # sequence numbers after card swaps/removals. Keep one suffix for all companions.
    # Try counters starting at 1 until all destinations are non-conflicting.
    # (Hard cap avoids infinite loops in weird cases.)
    for counter in range(1, MAX_DESTINATION_NAME_ATTEMPTS + 1):
        chosen = {}
        ok = True
        for ft, basename in orig.items():
            candidate_basename = add_counter_suffix(basename, counter)
            candidate_path = os.path.join(dest_dir, candidate_basename)
            # Paths claimed by this invocation are never overwriteable, even
            # with --force.  --force only applies to pre-existing files.
            if reserved_paths and reservation_key(candidate_path) in reserved_paths:
                ok = False
                break
            if dest_conflicts(
                srcs[ft],
                candidate_path,
                force=force,
                destination_checks=destination_checks,
            ):
                ok = False
                break
            chosen[ft] = candidate_basename
        if ok:
            return counter, chosen

    # Every candidate conflicted.  Fail loudly rather than returning a name
    # that was just rejected: callers must skip the stem instead of risking an
    # overwrite.  (Extremely unlikely in practice.)
    raise DestinationNameExhausted(dest_dir, orig.values())


def print_collision_rename_notice(
    dest_dir: str, stem_label: str, changes: list[tuple[str, str]], dry_run: bool
) -> None:
    """
    Always prints (even without --verbose) when we rename due to destination collisions.
    """
    if not changes:
        return
    verb = "Would rename" if dry_run else "Renaming"
    print(
        f"Note: {verb} files for '{stem_label}' in '{dest_dir}' due to a filename "
        "collision. Cameras can reuse numbers after card swaps or numbering resets; "
        "Stackcopy preserves the existing photo."
    )
    for old, new in changes:
        print(f"  - {old} -> {new}")


class DurabilityError(OSError):
    """A required durability operation failed.

    Raised instead of silently continuing, so a cross-device move can leave the
    source untouched rather than deleting irreplaceable camera files whose copy
    is not known to have reached stable storage.
    """


# Errnos meaning "this filesystem does not implement fsync on directories"
# rather than "the data is at risk".  exFAT/FAT card mounts, CIFS shares and
# WSL's 9P bridge routinely land here; the file contents are already durable,
# only the directory entry could not be confirmed.
#
# Every entry is a documented "this object cannot be fsynced" signal - EINVAL
# is the POSIX one.  Permission errors are deliberately absent: EPERM and
# EACCES say we were denied, which is not proof that the operation is
# unimplemented, and an unexplained denial is exactly the ambiguous case where
# an irreplaceable camera original must be kept rather than deleted.  A
# conservative false alarm on an odd filesystem is the cheaper mistake.
_DIRECTORY_FSYNC_UNSUPPORTED_ERRNOS = frozenset(
    getattr(errno, name)
    for name in ("EINVAL", "ENOTSUP", "EOPNOTSUPP", "ENOSYS")
    if hasattr(errno, name)
)

_directory_fsync_warning_shown = False


def directory_fsync_supported() -> bool:
    """True when this platform can fsync a directory at all.

    Windows has no way to open a directory as a file descriptor, and NTFS
    journals its own metadata, so the rename is not made durable by hand
    there.  This must not turn into an import failure on Windows.
    """
    return not IS_WINDOWS and hasattr(os, "O_DIRECTORY")


def fsync_file(fd: int) -> None:
    """Flush an already-open file's contents to stable storage.

    Takes a descriptor rather than a path on purpose.  The caller opens the
    temp file for writing *before* stamping the source's metadata onto it, so
    a read-only camera original can never leave Stackcopy unable to reopen its
    own copy for the flush.  A writable descriptor is also what Windows needs,
    where ``os.fsync`` is implemented via ``_commit()``.
    """
    os.fsync(fd)


class DirectorySync(Enum):
    """Outcome of trying to make a renamed directory entry durable."""

    SYNCED = "synced"
    # The platform or filesystem has no way to do this (Windows, exFAT card
    # mounts, CIFS, WSL's 9P bridge).  The file contents are already flushed,
    # which is as durable as this platform gets, so a move may still complete.
    UNSUPPORTED = "unsupported"
    # A real I/O error on a filesystem that does support it — the destination
    # filesystem is in trouble, so the source must be kept.
    FAILED = "failed"


def fsync_directory(path: str) -> DirectorySync:
    """Make a directory entry durable, reporting what the platform managed."""
    global _directory_fsync_warning_shown
    if not directory_fsync_supported():
        return DirectorySync.UNSUPPORTED
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except OSError as open_error:
        if open_error.errno in _DIRECTORY_FSYNC_UNSUPPORTED_ERRNOS:
            return _note_directory_fsync_unsupported(path, open_error)
        print(
            f"Error: could not open directory '{display_path(path)}' to flush "
            f"it: {open_error}"
        )
        return DirectorySync.FAILED
    try:
        os.fsync(fd)
    except OSError as sync_error:
        if sync_error.errno in _DIRECTORY_FSYNC_UNSUPPORTED_ERRNOS:
            return _note_directory_fsync_unsupported(path, sync_error)
        print(
            f"Error: could not flush directory '{display_path(path)}' to "
            f"storage: {sync_error}"
        )
        return DirectorySync.FAILED
    finally:
        os.close(fd)
    return DirectorySync.SYNCED


def _note_directory_fsync_unsupported(path: str, error: OSError) -> DirectorySync:
    global _directory_fsync_warning_shown
    if not _directory_fsync_warning_shown:
        _directory_fsync_warning_shown = True
        print(
            f"Note: this filesystem cannot flush directory entries "
            f"('{display_path(path)}': {error.strerror or error}). File "
            "contents are still flushed before any source file is deleted."
        )
    return DirectorySync.UNSUPPORTED


def _atomic_copy2(src_path: str, dest_path: str, durable: bool = False) -> bool:
    """Copy to a temp file in dest dir, then atomically replace dest_path.

    With ``durable=True`` the copied bytes are flushed to stable storage
    *before* the rename, and the renamed directory entry is flushed after it.
    Returns True when it is safe to delete the source: either the directory
    entry was flushed, or this platform/filesystem has no way to flush one and
    the (already durable) file contents are the best guarantee available.
    Callers about to delete the source must pass ``durable=True``; a plain copy
    leaves the source in place and does not need the cost.
    """
    dest_dir = os.path.dirname(dest_path)
    os.makedirs(dest_dir, exist_ok=True)
    tmp_path = os.path.join(
        dest_dir, f".__stackcopy_tmp__{os.path.basename(dest_path)}.{uuid.uuid4().hex}"
    )
    try:
        shutil.copyfile(src_path, tmp_path)
        # Order matters: the writable descriptor used for the durability flush
        # is opened *before* copystat stamps the source's mode bits onto the
        # temp file.  Camera originals are often write-protected, and applying
        # those bits first would make a reopen-to-fsync fail with EACCES on a
        # copy that had in fact succeeded.  Holding the descriptor across
        # copystat keeps the flush independent of the resulting permissions,
        # while the destination still ends up with the source's metadata.
        temp_fd = None
        if durable:
            try:
                temp_fd = os.open(tmp_path, os.O_RDWR)
            except OSError as open_error:
                raise DurabilityError(
                    open_error.errno,
                    f"could not open the copy of '{dest_path}' to flush it: "
                    f"{open_error}",
                ) from open_error
        try:
            shutil.copystat(src_path, tmp_path)
            if temp_fd is not None:
                try:
                    fsync_file(temp_fd)
                except OSError as sync_error:
                    raise DurabilityError(
                        sync_error.errno,
                        f"could not flush '{dest_path}' to storage: {sync_error}",
                    ) from sync_error
        finally:
            if temp_fd is not None:
                os.close(temp_fd)
        # Atomic replace: dest_path is either old or new, never partial
        os.replace(tmp_path, dest_path)
    finally:
        # Clean up temp if something went wrong before replace
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass

    if not durable:
        return True
    return fsync_directory(dest_dir) is not DirectorySync.FAILED


# Destinations that existed before this invocation and were replaced (or, in a
# dry run, would be replaced) because --force was given.  Module-level so every
# operation mode reports the same tally; reset at the start of each run.
forced_overwrite_paths: list[str] = []

# Sources whose data reached the destination but which could not be removed.
# Tallied at the point the outcome is decided so no call site can forget it.
source_remains_paths: list[str] = []


def safe_file_operation(
    operation,
    src_path,
    dest_path,
    operation_name,
    force=False,
    dry_run=False,
    destination_check: DestinationCheck | None = None,
):
    """
    Perform a safe file copy or move.
    Returns: (OperationResult, bytes_copied_count)

    The result is truthy whenever the destination was placed, so callers that
    only ask "did this land?" need no changes; callers that must distinguish a
    finished move from a copy whose source could not be removed read
    ``result.outcome``.
    """
    succeeded = OperationResult(OperationOutcome.SUCCESS)

    # Determine size before operation in case of move/deletion
    src_size = 0
    if os.path.exists(src_path):
        try:
            src_size = os.path.getsize(src_path)
        except OSError:
            pass

    check = refresh_destination_check(src_path, dest_path, destination_check)

    # The same-file case is classified as identical, but must remain a no-op:
    # deleting the source of a move would delete the destination too.  Inode
    # identity is only consulted where it is meaningful (see
    # is_same_physical_file); otherwise the paths themselves decide.
    if check.state == DestinationState.IDENTICAL and is_same_physical_file(
        src_path, dest_path, check
    ):
        return succeeded, 0

    # --force permits replacement of conflicting content; it does not require
    # rewriting a destination that is already identical.
    if check.state == DestinationState.IDENTICAL:
        if dry_run:
            if operation == "move":
                print(
                    "Would delete source because identical destination already "
                    f"exists: '{src_path}'"
                )
            else:
                print(
                    "Would skip copy because identical destination already "
                    f"exists: '{dest_path}'"
                )
            return succeeded, 0

        if operation == "move":
            try:
                os.unlink(src_path)
            except OSError as delete_error:
                # The destination already holds this photo, so this is not a
                # failure to retry or recover — it is a move that stopped one
                # step short, and the card still holds the original.
                print(
                    f"Note: destination '{dest_path}' already exists with identical content; "
                    f"source '{src_path}' could not be deleted: {delete_error}"
                )
                source_remains_paths.append(src_path)
                return OperationResult(OperationOutcome.COPIED_SOURCE_REMAINS), 0
            print(
                f"Note: destination '{dest_path}' already exists with identical content; "
                f"deleted source '{src_path}'."
            )
            return succeeded, 0

        print(
            f"Note: destination '{dest_path}' already exists with identical content; "
            f"skipping copy from '{src_path}'."
        )
        return succeeded, 0

    forced_overwrite = False
    if check.state == DestinationState.ZERO_BYTE_RECOVERABLE and not force:
        msg = "replacing from source" if not dry_run else "would replace from source"
        print(f"Note: destination '{dest_path}' exists but is 0 bytes; {msg}.")
        force = True
    elif check.state == DestinationState.CONFLICT:
        if not force:
            if dry_run:
                print(
                    f"Warning: '{dest_path}' already exists. Would need --force to overwrite."
                )
            else:
                print(
                    f"Warning: '{dest_path}' already exists. Use --force to overwrite."
                )
            return OperationResult(OperationOutcome.FAILED), 0
        # Destroying a pre-existing, differing file is the one destructive
        # thing Stackcopy does.  Always say so, with or without --verbose, and
        # tally it here so no mode can forget to report it.
        forced_overwrite = True
        verb = "Would overwrite" if dry_run else "Overwriting"
        print(f"{verb} differing existing file because --force:")
        print(f"  {dest_path}")

    def placed(outcome=OperationOutcome.SUCCESS) -> OperationResult:
        """Build a result for a destination that was actually written.

        The forced-overwrite tally is recorded here rather than at the point
        of decision, so a copy that then fails - leaving the pre-existing
        destination intact - is never counted as an overwrite.
        """
        if forced_overwrite:
            forced_overwrite_paths.append(dest_path)
        if outcome is OperationOutcome.COPIED_SOURCE_REMAINS:
            source_remains_paths.append(src_path)
        return OperationResult(outcome, forced_overwrite=forced_overwrite)

    if dry_run:
        return placed(), 0

    try:
        if operation == "move":
            try:
                # Same-filesystem fast path (metadata only).  Nothing is
                # copied, so no fsync is warranted here.
                os.replace(src_path, dest_path)
                return placed(), 0
            except OSError as move_error:
                if move_error.errno == errno.EXDEV:
                    # Cross-device: durable copy, then delete the source.  The
                    # source is never deleted before the destination bytes are
                    # known to have reached storage.
                    source_may_be_deleted = _atomic_copy2(
                        src_path, dest_path, durable=True
                    )
                    if not source_may_be_deleted:
                        print(
                            f"Note: {operation_name} '{src_path}' to '{dest_path}' copied "
                            "the data, but its durability could not be confirmed; "
                            "the source was left in place."
                        )
                        return (
                            placed(OperationOutcome.COPIED_SOURCE_REMAINS),
                            src_size,
                        )
                    try:
                        os.unlink(src_path)
                    except OSError as delete_error:
                        print(
                            f"Note: {operation_name} '{src_path}' to '{dest_path}' completed, "
                            f"but source could not be deleted: {delete_error}"
                        )
                        return (
                            placed(OperationOutcome.COPIED_SOURCE_REMAINS),
                            src_size,
                        )
                    # For cross-device moves, bytes were physically moved
                    return placed(), src_size
                else:
                    raise
        elif operation == "copy":
            # The source stays put, so the extra flush would buy nothing.
            _atomic_copy2(src_path, dest_path)
            return placed(), src_size

        # Unknown operation: nothing was written, so nothing is tallied.
        return OperationResult(OperationOutcome.SUCCESS), 0

    except DurabilityError as durability_error:
        print(
            f"Error {operation_name} '{src_path}' to '{dest_path}': "
            f"{durability_error.strerror or durability_error}. "
            "The source was left untouched."
        )
        return OperationResult(OperationOutcome.FAILED), 0
    except (OSError, shutil.Error) as e:
        print(f"Error {operation_name} '{src_path}' to '{dest_path}': {e}")
        return OperationResult(OperationOutcome.FAILED), 0


def format_bytes(n: int) -> str:
    """Format bytes into human readable string (KiB, MiB, etc)."""
    # Special case for bytes to avoid decimals (e.g. "12 B" not "12.0 B")
    if abs(n) < 1024:
        return f"{int(n)} B"

    val = float(n) / 1024.0
    for unit in ["KiB", "MiB", "GiB", "TiB"]:
        if abs(val) < 1024.0:
            return f"{val:3.1f} {unit}"
        val /= 1024.0
    return f"{val:.1f} PiB"


def get_existing_parent(path: str) -> str | None:
    """Return the nearest existing parent directory for a path."""
    try:
        path = os.path.abspath(path)
        while not os.path.exists(path):
            parent = os.path.dirname(path)
            if parent == path:  # Root reached and doesn't exist? Unlikely.
                return None
            path = parent
        return path
    except OSError:
        return None


def get_device_id(path: str) -> int | None:
    """Get the device ID for a path, walking up if it doesn't exist."""
    existing_path = get_existing_parent(path)
    if existing_path:
        try:
            return os.stat(existing_path).st_dev
        except OSError:
            return None
    return None


def estimate_required_bytes_for_ops(ops: list[tuple[str, str, str]]) -> dict[int, dict]:
    """
    Estimate space requirements for operations.
    ops: list of (src_path, dest_path, op_type) where op_type is 'move' or 'copy'.
    Returns: {device_id: {'bytes': int, 'count': int, 'sample_path': str}}
    """
    req_map = defaultdict(lambda: {"bytes": 0, "count": 0, "sample_path": None})

    for src_path, dest_path, op_type in ops:
        # 1. Get source size and device
        try:
            src_stat = os.stat(src_path)
            src_size = src_stat.st_size
            src_dev = src_stat.st_dev
        except OSError:
            # If source is missing/unreadable, we can't estimate size.
            # Treat as 0 bytes to avoid crashing.
            src_size = 0
            src_dev = None

        # 2. Get destination device
        dest_dev = get_device_id(dest_path)
        if dest_dev is None:
            continue

        # 3. Determine if this writes to destination
        writes_to_dest = False
        if op_type == "copy":
            writes_to_dest = True
        elif op_type == "move" and (src_dev is None or src_dev != dest_dev):
            # If we can't determine source device, assume cross-device (safest)
            writes_to_dest = True

        if writes_to_dest:
            info = req_map[dest_dev]
            info["bytes"] += src_size
            info["count"] += 1
            if info["sample_path"] is None:
                info["sample_path"] = dest_path

    return req_map


# Cache for confirmed filesystems to avoid repeated prompts
_confirmed_filesystems = set()


def _abort_low_space_without_confirmation(quiet: bool = False) -> None:
    if not quiet:
        print(
            "Refusing to proceed: destination space is low and no TTY is available to confirm."
        )
    sys.exit(1)


def confirm_if_low_space(ops: list[tuple[str, str, str]], dry_run: bool) -> None:
    """
    Check if destination filesystems have enough space. Prompt user if low.
    """
    required_map = estimate_required_bytes_for_ops(ops)

    for dev_id, info in required_map.items():
        if dev_id in _confirmed_filesystems:
            continue

        req_bytes = info["bytes"]
        count = info["count"]
        sample_path = info["sample_path"]
        if not sample_path:
            continue

        # Get free space
        check_path = get_existing_parent(sample_path)
        if not check_path:
            continue

        try:
            usage = shutil.disk_usage(check_path)
            free_bytes = usage.free
            total_bytes = usage.total
        except OSError:
            continue

        # Threshold: max(2 GiB, 5% of total) capped at 50 GiB for large drives
        reserve_bytes = max(2 * 1024**3, min(int(total_bytes * 0.05), 50 * 1024**3))

        estimated_free = free_bytes - req_bytes

        is_low = (req_bytes > free_bytes) or (estimated_free < reserve_bytes)

        if is_low:
            report = {
                "dry_run": dry_run,
                "sample_path": sample_path,
                "destination": check_path,
                "free_bytes": free_bytes,
                "required_bytes": req_bytes,
                "estimated_free_bytes": estimated_free,
                "reserve_bytes": reserve_bytes,
                "count": count,
                "free": format_bytes(free_bytes),
                "required": format_bytes(req_bytes),
                "estimated_free": format_bytes(estimated_free),
                "reserve": format_bytes(reserve_bytes),
                "shortfall": (
                    format_bytes(-estimated_free) if estimated_free < 0 else None
                ),
            }

            if os.environ.get("STACKCOPY_ASSUME_YES") == "1" and not dry_run:
                if not _LOW_SPACE_REPORTS_ENABLED:
                    print(
                        "Proceeding despite low space (confirmed via STACKCOPY_ASSUME_YES)."
                    )
                _confirmed_filesystems.add(dev_id)
                continue

            if _LOW_SPACE_REPORTS_ENABLED:
                _emit_low_space_report(report)
                if not dry_run:
                    _abort_low_space_without_confirmation(quiet=True)

            header = "DRY RUN WARNING" if dry_run else "WARNING"
            print(
                f"\n{header}: Low disk space detected on destination device for '{sample_path}'"
            )
            print(f"  Destination filesystem: {check_path}")
            print(f"  Current free space:     {format_bytes(free_bytes)}")
            print(f"  Required ({count} files):   {format_bytes(req_bytes)}")

            if estimated_free < 0:
                print(
                    f"  Est. free after ops:    {format_bytes(estimated_free)} (OVERFLOW by {format_bytes(-estimated_free)})"
                )
            else:
                print(f"  Est. free after ops:    {format_bytes(estimated_free)}")

            print(f"  Reserve threshold:      {format_bytes(reserve_bytes)}")

            if dry_run:
                # A preview performs no writes and never needs confirmation.
                continue

            if not sys.stdin.isatty():
                _abort_low_space_without_confirmation()

            try:
                response = input("  Proceed anyway? [y/N] ").strip().lower()
            except EOFError:
                print()
                _abort_low_space_without_confirmation()
            if response not in ("y", "yes"):
                print("Aborted by user.")
                sys.exit(1)

            _confirmed_filesystems.add(dev_id)


def is_cross_device(src_path, dest_path):
    """Check if source and destination are on different devices."""
    try:
        return os.stat(src_path).st_dev != os.stat(dest_path).st_dev
    except OSError:
        return True  # Assume cross-device if we can't tell


def format_action_message(
    operation_mode, filename, dest_filename, dest_dir, success, dry_run, used_prefix
):
    """Generate consistent action messages for all operations."""
    if (
        operation_mode == "rename"
        or operation_mode == "lightroom"
        or operation_mode == "lightroomimport"
    ):
        if dry_run:
            action = "Would rename"
        else:
            action = "Renamed" if success else "Failed to rename"
        return f"{action} '{filename}' to '{dest_filename}'"
    else:  # copy or stackcopy
        if operation_mode == "stackcopy" or used_prefix:
            if dry_run:
                action = "Would copy and rename"
            else:
                action = (
                    "Copied and renamed" if success else "Failed to copy and rename"
                )
        else:
            if dry_run:
                action = "Would copy"
            else:
                action = "Copied" if success else "Failed to copy"
        return (
            f"{action} '{filename}' to '{dest_filename}' in '{display_path(dest_dir)}'"
        )


def parse_stacked_image_value(value: Any) -> StackMetadata:
    """Interpret ExifTool's Olympus StackedImage value conservatively."""
    raw_value = None if value is None else str(value)
    numbers: list[int] | None = None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        try:
            numbers = [int(value[0]), int(value[1])]
        except (TypeError, ValueError):
            numbers = None
    elif isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+)\s+(\d+)\s*", value)
        if match:
            numbers = [int(match.group(1)), int(match.group(2))]
        else:
            focus_match = re.fullmatch(
                r"\s*Focus-stacked\s*\(\s*(\d+)\s+images?\s*\)\s*",
                value,
                flags=re.IGNORECASE,
            )
            if focus_match:
                count = int(focus_match.group(1))
                if count > 0:
                    return StackMetadata(
                        StackMetadataState.FOCUS_STACK, count, raw_value
                    )
            if value.strip().casefold() == "no":
                return StackMetadata(
                    StackMetadataState.NOT_FOCUS_STACK, None, raw_value
                )

    if numbers is None:
        return StackMetadata(StackMetadataState.UNKNOWN, None, raw_value)
    mode, count = numbers
    if mode == 9 and count > 0:
        return StackMetadata(StackMetadataState.FOCUS_STACK, count, raw_value)
    known_non_focus_modes = {1, 3, 4, 5, 6, 8, 11, 13}
    if (mode, count) == (0, 0) or mode in known_non_focus_modes:
        return StackMetadata(StackMetadataState.NOT_FOCUS_STACK, None, raw_value)
    # Preserve fallback behavior for future/unrecognized numeric modes.
    return StackMetadata(StackMetadataState.UNKNOWN, None, raw_value)


def read_stacked_image_metadata(jpeg_paths: list[str]) -> dict[str, StackMetadata]:
    """Read Olympus StackedImage MakerNotes in bounded ExifTool batches.

    ExifTool is optional.  Missing executables, unsupported MakerNotes, absent
    tags, malformed output, and subprocess failures all produce UNKNOWN by
    omission so callers retain the heuristic fallback.
    """
    results = {path: StackMetadata(StackMetadataState.UNKNOWN) for path in jpeg_paths}
    exiftool = shutil.which("exiftool")
    if not exiftool or not jpeg_paths:
        return results

    normalized_to_path = {
        os.path.normcase(os.path.abspath(path)): path for path in jpeg_paths
    }
    # Stay comfortably below Windows' command-line limit while avoiding the
    # pathological one-process-per-JPEG pattern.
    batches: list[list[str]] = []
    batch: list[str] = []
    batch_characters = 0
    for path in jpeg_paths:
        path_characters = len(path) + 3
        if batch and (len(batch) >= 250 or batch_characters + path_characters > 24000):
            batches.append(batch)
            batch = []
            batch_characters = 0
        batch.append(path)
        batch_characters += path_characters
    if batch:
        batches.append(batch)

    # Reasons this invocation could not read every file, reported once at the
    # end rather than per batch or per file.  Absence of the tag on a healthy
    # file is normal and never lands here.
    degraded_reasons: list[str] = []
    degraded_files = 0

    def note_degraded(reason: str, file_count: int) -> None:
        nonlocal degraded_files
        if reason not in degraded_reasons:
            degraded_reasons.append(reason)
        degraded_files += file_count

    for batch in batches:
        try:
            completed = subprocess.run(
                [
                    exiftool,
                    "-json",
                    "-n",
                    "-Olympus:StackedImage",
                    *batch,
                ],
                check=False,
                capture_output=True,
                stdin=subprocess.DEVNULL,
                encoding="utf-8",
                errors="replace",
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            note_degraded("a batch timed out", len(batch))
            continue
        except (OSError, subprocess.SubprocessError):
            note_degraded("ExifTool could not be run", len(batch))
            continue

        # A nonzero exit usually means *one* file in the batch failed, while
        # valid JSON was still emitted for the rest.  Discarding the whole
        # batch would drop hundreds of good results, so parse regardless and
        # let per-entry errors decide which files stay UNKNOWN.
        try:
            payload = json.loads(completed.stdout)
        except (json.JSONDecodeError, TypeError, ValueError):
            payload = None
        if not isinstance(payload, list):
            note_degraded("ExifTool output could not be parsed", len(batch))
            continue
        reported_paths: set[str] = set()

        for item in payload:
            if not isinstance(item, dict):
                continue
            source_file = item.get("SourceFile")
            if not isinstance(source_file, str):
                continue
            original_path = normalized_to_path.get(
                os.path.normcase(os.path.abspath(source_file))
            )
            if original_path is None:
                continue
            reported_paths.add(original_path)
            if item.get("Error"):
                # This one file is unreadable; it stays UNKNOWN so the
                # heuristic still gets its chance.
                note_degraded("ExifTool reported an error for some files", 1)
                continue
            # -G options may qualify keys, so accept a final component match.
            tag_value = None
            tag_found = False
            for key, candidate_value in item.items():
                if key.rsplit(":", 1)[-1] == "StackedImage":
                    tag_value = candidate_value
                    tag_found = True
                    break
            if tag_found:
                results[original_path] = parse_stacked_image_value(tag_value)

        if completed.returncode != 0:
            # Files ExifTool never reported on at all still lost their chance
            # at metadata; absence of the tag on a reported file has not.
            unreported = sum(1 for path in batch if path not in reported_paths)
            if unreported:
                note_degraded("ExifTool reported an error for some files", unreported)

    if degraded_files:
        detail = "; ".join(degraded_reasons) if degraded_reasons else "some files"
        print(
            f"Warning: camera stack metadata could not be read for "
            f"{degraded_files} file(s) ({detail}). Those files fall back to "
            "heuristic stack detection."
        )

    return results


def main():
    """Main program entry point."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Process JPG files without corresponding raw files"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"Stackcopy {STACKCOPY_VERSION}",
    )

    # Create a mutually exclusive group for the three operation modes
    mode_group = parser.add_mutually_exclusive_group()

    # Copy mode - requires source and destination
    mode_group.add_argument(
        "--copy",
        nargs=2,
        metavar=("SRC_DIR", "DEST_DIR"),
        help="Copy JPG files without matching raw files from source to destination. Can be used with --prefix.",
    )

    # Rename mode - optional directory argument (renames files in-place)
    mode_group.add_argument(
        "--rename",
        "-r",
        nargs="?",
        const=os.getcwd(),
        metavar="DIR",
        help="Rename JPG files without matching raw files in-place by adding ' stacked' (defaults to current directory)",
    )

    # Stack copy mode - optional directory argument (copies to 'stacked' subdirectory)
    mode_group.add_argument(
        "--stackcopy",
        nargs="?",
        const=os.getcwd(),
        metavar="DIR",
        help="Copy JPG files without matching raw files to a 'stacked' subdirectory with ' stacked' added to filenames",
    )

    # Lightroom mode - optional directory argument
    mode_group.add_argument(
        "--lightroom",
        nargs="?",
        const=os.getcwd(),
        metavar="DIR",
        help="Move stack input files (JPG, RAW, and ORI) to a dated directory structure and rename the stacked JPG in place.",
    )

    # Lightroom import mode - optional directory argument
    mode_group.add_argument(
        "--lightroomimport",
        nargs="?",
        const=os.getcwd(),
        metavar="DIR",
        help=(
            "Same as --lightroom, but scans recursively and moves remaining photos and videos to a dated directory structure "
            f"under the user's Pictures directory (default: {os.path.join(_default_pictures_dir(), 'Lightroom')}/YEAR/DATE/). "
            "The destination can be overridden via the STACKCOPY_LIGHTROOM_IMPORT_DIR environment variable."
        ),
    )

    # Add date filtering options
    date_group = parser.add_argument_group("Date Filtering (optional)")
    date_group.add_argument(
        "--today",
        action="store_true",
        help="Only process media whose file modification date is today.",
    )
    date_group.add_argument(
        "--yesterday",
        action="store_true",
        help="Only process media whose file modification date is yesterday.",
    )
    date_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Only process media with the specified file modification date.",
    )

    # Add prefix option
    parser.add_argument(
        "--prefix",
        metavar="PREFIX",
        help=(
            "Add custom text before ' stacked' in output filenames for --copy, "
            "--rename, --stackcopy, --lightroom, and --lightroomimport."
        ),
    )

    # Add dry-run option
    parser.add_argument(
        "--dry",
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Show what would happen without making any actual changes",
    )

    # Add verbose flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed information about processed files",
    )

    # Add overwrite protection option
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files without prompting",
    )

    # Add debug flag for stack detection
    parser.add_argument(
        "--debug-stacks",
        "--debugstacks",
        dest="debug_stacks",
        action="store_true",
        help="Enable detailed diagnostic output for stack detection",
    )

    parser.add_argument(
        "--no-stack-detection",
        action="store_true",
        help="Skip automatic stack detection in --lightroom and --lightroomimport.",
    )

    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Show a summary and ask for confirmation before moving files (--lightroomimport only)",
    )

    parser.add_argument(
        "--leave-on-card",
        action="store_true",
        help="Copy files during --lightroomimport instead of moving them, leaving source files on the card.",
    )

    parser.add_argument(
        "--plan-json",
        action="store_true",
        help=(
            "Scan and plan --lightroomimport without changing files, then emit "
            "one machine-readable JSON object."
        ),
    )

    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Parallel workers for --copy/--stackcopy (default: 1). "
            "--lightroom auto-selects up to 4; --lightroomimport is sequential. "
            "Values are capped at 2x CPU count."
        ),
    )

    # Parse arguments
    # --- 0. Execution Tracking & Summary Statistics ---
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    primary_failure_count = 0
    recovered_count = 0
    moved_input_count = 0
    moved_output_count = 0
    moved_stack_groups: set[str] = set()
    stack_group_of_stem: dict[str, str] = {}
    stack_outputs_seen = 0
    remaining_moved_count = 0
    inputs_not_all_raw_backed_skipped = 0
    total_bytes_moved = 0
    exec_start_time = None
    exec_elapsed_time = 0
    partial_failures_found = False
    execution_results: dict[str, dict] = {}
    interrupted = False
    interrupted_remaining = 0
    # Stack-output candidates --lightroom did not finish processing (the one
    # interrupted mid-rename plus those never reached).  Reported separately
    # because only some candidates become real operations.
    unprocessed_output_candidates = 0
    name_exhausted_count = 0
    ambiguous_file_count = 0
    # Destructive and degraded outcomes, tracked separately from ordinary
    # successes so the summary can never round them away.
    forced_overwrite_paths.clear()
    source_remains_paths.clear()
    recovered_source_remains_count = 0
    recovery_dest_dirs: set[str] = set()
    # Legacy --lightroom input moves whose destination was written but whose
    # source could not be removed.  Tracked separately so they are never
    # counted as moved.
    lightroom_source_remains_count = 0

    args = parser.parse_args()

    # A plan request reuses the real planner with dry-run filesystem semantics.
    # Route incidental diagnostics to stderr so stdout remains exactly one JSON
    # object for callers. The original stream is restored when the payload is
    # emitted below.
    plan_json_stdout = None
    if args.plan_json:
        if args.lightroomimport is None:
            parser.error("--plan-json can only be used with --lightroomimport.")
        args.dry_run = True
        plan_json_stdout = sys.stdout
        sys.stdout = sys.stderr

    if args.debug_stacks:
        print(f"Running Stackcopy from: {os.path.abspath(__file__)}")

    if args.jobs < 1:
        parser.error("--jobs must be at least 1.")

    # Clamp number of jobs to a reasonable limit
    cpu_count = os.cpu_count() or 1
    if args.jobs > cpu_count * 2:
        if args.verbose:
            print(
                f"Warning: --jobs reduced from {args.jobs} to {cpu_count * 2} (2x CPU cores) to avoid resource exhaustion."
            )
        args.jobs = cpu_count * 2

    if args.lightroom and not args.dry_run and args.jobs == 1:
        # When the effective count is still one, pick something sensible.
        # 4 workers max, but don't exceed 2x CPU cores
        auto_jobs = min(4, cpu_count * 2)
        if args.verbose:
            print(f"Auto-selecting {auto_jobs} worker jobs for Lightroom mode.")
        args.jobs = auto_jobs

    # lightroomimport always runs sequentially so files move in oldest-first order
    if args.lightroomimport is not None:
        args.jobs = 1

    created_dirs = set()

    prefix_error = prefix_validation_error(args.prefix)
    if prefix_error:
        print(f"Error: {prefix_error}")
        sys.exit(1)

    # Determine the target date for filtering
    target_date = None
    if args.today:
        target_date = date.today()
    elif args.yesterday:
        target_date = date.today() - timedelta(days=1)
    elif args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(
                f"Error: Date format for --date must be YYYY-MM-DD. You provided '{args.date}'."
            )
            sys.exit(1)

    # If no operation mode is specified, show help and exit
    if (
        not args.copy
        and args.rename is None
        and args.stackcopy is None
        and args.lightroom is None
        and args.lightroomimport is None
    ):
        parser.print_help()
        sys.exit(1)

    if args.leave_on_card and args.lightroomimport is None:
        parser.error("--leave-on-card can only be used with --lightroomimport.")

    if (
        args.no_stack_detection
        and args.lightroom is None
        and args.lightroomimport is None
    ):
        parser.error(
            "--no-stack-detection can only be used with --lightroom or --lightroomimport."
        )

    # Determine operation mode and set directories
    if args.copy:
        operation_mode = "copy"
        src_dir = normalize_path(args.copy[0])
        dest_dir = normalize_path(args.copy[1])

        # Verify that the source directory exists
        if not os.path.isdir(src_dir):
            print(
                f"Error: Source directory '{src_dir}' does not exist or is not a directory."
            )
            sys.exit(1)

        # Check if source and destination are the same
        if paths_are_same(src_dir, dest_dir):
            print("Error: Source and destination directories cannot be the same.")
            sys.exit(1)

        # Ensure the destination directory exists, create if necessary (but not in dry run)
        try:
            ensure_directory_once(dest_dir, created_dirs, args.dry_run)
        except OSError as e:
            print(f"Error creating destination directory '{dest_dir}': {e}")
            sys.exit(1)
    elif args.rename is not None:  # --rename mode
        operation_mode = "rename"
        work_dir = normalize_path(args.rename)

        # Verify that the specified directory exists
        if not os.path.isdir(work_dir):
            print(
                f"Error: Directory '{work_dir}' does not exist or is not a directory."
            )
            sys.exit(1)

        # For rename mode, source and working directory are the same
        src_dir = work_dir
        dest_dir = work_dir  # We're renaming in-place
    elif (
        args.lightroom is not None or args.lightroomimport is not None
    ):  # --lightroom mode
        operation_mode = (
            "lightroom" if args.lightroom is not None else "lightroomimport"
        )
        work_dir = normalize_path(
            args.lightroom if args.lightroom is not None else args.lightroomimport
        )

        # Verify that the specified directory exists
        if not os.path.isdir(work_dir):
            print(
                f"Error: Directory '{work_dir}' does not exist or is not a directory."
            )
            sys.exit(1)

        # For lightroom mode, source and working directory are the same
        src_dir = work_dir
        dest_dir = work_dir  # We're renaming in-place

        # Importing *out of* a destination tree would re-sort files Stackcopy
        # has already filed, renaming and re-dating them.  The recursive scan
        # already refuses to descend into its own destinations; this is the
        # inverse case, and it has to be caught before anything is scanned or
        # moved.  Real paths are compared so a symlink or an alternative
        # spelling cannot slip past.
        if operation_mode == "lightroomimport":
            import_destination_error = lightroom_import_source_conflict(
                src_dir, _lightroom_import_base_dir(), STACK_INPUT_DIR
            )
            if import_destination_error:
                print(import_destination_error)
                sys.exit(1)

        # Lightroom Import creates dated destinations during execution so a
        # per-operation mkdir failure can be reported without losing its plan
        # and final summary.  Lightroom's legacy flow still needs its base now.
        if operation_mode == "lightroom":
            try:
                ensure_directory_once(STACK_INPUT_DIR, created_dirs, args.dry_run)
            except OSError as e:
                print(f"Error creating stack input directory '{STACK_INPUT_DIR}': {e}")
                sys.exit(1)
    else:  # --stackcopy mode
        operation_mode = "stackcopy"
        work_dir = normalize_path(args.stackcopy)

        # Verify that the specified directory exists
        if not os.path.isdir(work_dir):
            print(
                f"Error: Directory '{work_dir}' does not exist or is not a directory."
            )
            sys.exit(1)

        # For stackcopy mode, source is the working directory
        src_dir = work_dir
        # Create a 'stacked' subdirectory for the copies
        dest_dir = os.path.join(work_dir, "stacked")
        try:
            ensure_directory_once(dest_dir, created_dirs, args.dry_run)
        except OSError as e:
            print(f"Error creating stacked directory '{dest_dir}': {e}")
            sys.exit(1)

    # WSL performance warning — fires once if any involved path crosses the 9P bridge
    wsl_check_paths = [src_dir, dest_dir]
    if operation_mode in ("lightroom", "lightroomimport"):
        wsl_check_paths.append(STACK_INPUT_DIR)
        if operation_mode == "lightroomimport":
            # For lightroomimport, also check the base import directory
            wsl_check_paths.append(_lightroom_import_base_dir())

    _warn_wsl_performance(wsl_check_paths, operation_mode)

    # Define a list of common media extensions
    RAW_EXTENSIONS = {
        ".orf",
        ".cr2",
        ".nef",
        ".arw",
        ".dng",
        ".pef",
        ".rw2",
        ".raf",
        ".raw",
        ".sr2",
    }
    ORI_EXTENSIONS = {".ori"}
    JPG_EXTENSIONS = {".jpg", ".jpeg"}
    VIDEO_EXTENSIONS = {
        ".mov",
        ".mp4",
        ".m4v",
        ".avi",
        ".mts",
        ".m2ts",
        ".mpg",
        ".mpeg",
        ".wmv",
    }
    REMAINING_FILE_TYPES = ("jpg", "raw", "ori", "video")
    FILE_TYPE_LABELS = {
        "raw": "RAW",
        "ori": "ORI",
        "jpg": "JPG",
        "video": "video",
    }

    # --- 1. Scan directory and build file database ---
    # file_db stores metadata for each unique file stem found in the source directory.
    # Structure:
    # {
    #   "filename_stem": {
    #     "files": {
    #       "raw": {"path": "...", "mtime": datetime_obj, "date": date_obj},
    #       "jpg": {"path": "...", "mtime": datetime_obj, "date": date_obj},
    #       "video": {"path": "...", "mtime": datetime_obj, "date": date_obj}
    #     },
    #     "has_raw": bool,   # ordinary RAW only (ORF/CR2/NEF/ARW/...)
    #     "has_ori": bool,   # Olympus/OM .ORI companion (never sets has_raw)
    #     "has_jpg": bool,
    #     "has_video": bool,
    #     "numeric": {
    #       "prefix": "alpha_prefix",
    #       "num": int_sequence_number,
    #       "width": digit_width
    #     }
    #   },
    #   ...
    # }
    if operation_mode == "lightroomimport" and not args.plan_json:
        _emit_progress(phase="scan", done=0, total=0)
    scan_recursively = operation_mode == "lightroomimport"
    scan_exclude_dirs = ()
    if scan_recursively:
        # Don't descend into our own destination dirs when they live under
        # src_dir (e.g. running with no argument from ~/Pictures), which would
        # otherwise re-import already-sorted files on every run.
        scan_exclude_dirs = tuple(
            os.path.realpath(p) for p in (_lightroom_import_base_dir(), STACK_INPUT_DIR)
        )
    file_db = {}
    unrecognized_extensions: dict[str, int] = defaultdict(int)
    scanned_source_subdirs: set[str] = set()
    try:
        for entry in iter_source_file_entries(
            src_dir, recursive=scan_recursively, exclude_dirs=scan_exclude_dirs
        ):
            stem, ext = os.path.splitext(entry.name)
            ext_lower = ext.lower()
            if ext_lower in RAW_EXTENSIONS:
                scanned_file_type = "raw"
            elif ext_lower in ORI_EXTENSIONS:
                scanned_file_type = "ori"
            elif ext_lower in JPG_EXTENSIONS:
                scanned_file_type = "jpg"
            elif ext_lower in VIDEO_EXTENSIONS:
                scanned_file_type = "video"
            else:
                extension_label = ext.upper() if ext else "(no extension)"
                unrecognized_extensions[extension_label] += 1
                continue

            relative_dir = os.path.relpath(os.path.dirname(entry.path), src_dir)
            if relative_dir != ".":
                scanned_source_subdirs.add(relative_dir)
            record_key = (
                stem
                if not scan_recursively or relative_dir == "."
                else os.path.join(relative_dir, stem)
            )

            record = file_db.setdefault(
                record_key,
                {
                    "files": {},
                    "has_raw": False,
                    "has_ori": False,
                    "has_jpg": False,
                    "has_video": False,
                    "relative_dir": relative_dir,
                },
            )
            file_meta = {
                "path": entry.path,
                "basename": entry.name,
                "entry": entry,
                "mtime": None,
                "date": None,
            }  # Store entry object and basename

            if "numeric" not in record:
                numeric_match = NUMERIC_STEM_REGEX.fullmatch(stem)
                if numeric_match:
                    prefix, num_str = numeric_match.groups()
                    record["numeric"] = {
                        "prefix": prefix,
                        "num": int(num_str),
                        "width": len(num_str),
                    }

            # One stem holds at most one file of each logical type.  Two files
            # that map to the same type (P8081868.ORF + P8081868.DNG, or
            # X.JPG + X.JPEG) cannot both be represented, and silently keeping
            # one would leave the other on the card while the run still
            # reported success.  Record every physical file involved instead
            # and mark the stem unprocessable.
            existing = record["files"].get(scanned_file_type)
            if existing is not None:
                duplicates = record.setdefault("duplicate_types", {})
                paths = duplicates.setdefault(scanned_file_type, [existing["path"]])
                paths.append(entry.path)
                continue

            record["files"][scanned_file_type] = file_meta
            if scanned_file_type == "raw":
                record["has_raw"] = True
            elif scanned_file_type == "ori":
                # ORI is an Olympus/OM original companion (High Res Shot and
                # friends).  It is preserved as a first-class file and keeps its
                # own slot, but it is NOT ordinary RAW backing: only has_ori is
                # set, so files["raw"] and has_raw stay in sync.
                record["has_ori"] = True
            elif scanned_file_type == "jpg":
                record["has_jpg"] = True
            elif scanned_file_type == "video":
                record["has_video"] = True

    except OSError as e:
        print(f"Error scanning source directory '{src_dir}': {e}")
        sys.exit(1)

    # --- 1a. Stems that cannot be represented unambiguously ---
    # These are excluded from metadata reads, stack detection and planning, and
    # every one of their files is left exactly where it is.  The run fails so
    # the ambiguity cannot be mistaken for a clean import.
    ambiguous_stems = {
        stem for stem, record in file_db.items() if record.get("duplicate_types")
    }
    if ambiguous_stems:
        for stem in sorted(ambiguous_stems):
            ambiguous_record = file_db[stem]
            duplicate_types = ambiguous_record["duplicate_types"]
            for file_type, paths in sorted(duplicate_types.items()):
                ambiguous_file_count += len(paths)
                print(
                    f"Error: multiple {FILE_TYPE_LABELS.get(file_type, file_type)} "
                    f"files share stem '{os.path.basename(stem)}':"
                )
                for path in paths:
                    # Relative to the scan root, so a card with several camera
                    # folders still says which one is affected.
                    print(f"  {os.path.relpath(path, src_dir)}")
            # The whole stem stays on the card, not just the duplicated type,
            # so every other recognized file belonging to it counts too - a
            # JPG + ORF + DNG stem leaves three files behind, not two.  The
            # first path of each duplicate list is the one still occupying
            # that type's slot in record["files"], so those slots are skipped
            # here rather than counted twice.
            ambiguous_file_count += sum(
                1
                for file_type in ambiguous_record["files"]
                if file_type not in duplicate_types
            )
            print("Leaving this stem untouched.")

    stack_metadata_by_path: dict[str, StackMetadata] = {}
    if (
        operation_mode in ("lightroomimport", "lightroom")
        and not args.no_stack_detection
    ):
        jpeg_paths = [
            record["files"]["jpg"]["path"]
            for stem, record in file_db.items()
            if stem not in ambiguous_stems
            and record.get("has_jpg")
            and record["files"].get("jpg")
        ]
        stack_metadata_by_path = read_stacked_image_metadata(jpeg_paths)

    known_relative_dirs_by_key = {
        _relative_dir_lookup_key(record.get("relative_dir", ".")): record.get(
            "relative_dir", "."
        )
        for record in file_db.values()
    }

    sequences_by_prefix = {}
    for stem, record in file_db.items():
        if stem in ambiguous_stems:
            # An ambiguous stem must not anchor or extend a stack sequence:
            # a gap that stops the backward walk is the safe outcome.
            continue
        numeric_info = record.get("numeric")
        if numeric_info and (has_raw_like_companion(record) or record.get("has_jpg")):
            sequence_key = (record.get("relative_dir", "."), numeric_info["prefix"])
            sequences_by_prefix.setdefault(sequence_key, []).append(
                (numeric_info["num"], stem)
            )
    for prefix in sequences_by_prefix:
        sequences_by_prefix[prefix].sort()

    def get_stack_sequence(output_record, prefix):
        """Build the numeric sequence a stack output may draw its inputs from.

        A stack can legitimately straddle two adjacent camera folders, but the
        previous folder must only ever *extend* the sequence past the folder
        boundary.  It must never supply a number that belongs inside the
        current folder's own range: if the current folder's copy of that frame
        was deleted, an unrelated older photo carrying the same number would
        otherwise be adopted as a stack component and filed away as one.

        The rule is therefore: borrow from the previous folder only when its
        numbers all sit strictly below the current folder's lowest number.  Any
        overlap means the camera's numbering reset or the folders interleave,
        and nothing is borrowed at all.

        A DCF counter rollover (...9999 in one folder, 0001 in the next) is
        deliberately not special-cased, because it cannot produce a usable
        sequence here anyway.  NUMERIC_STEM_REGEX puts the whole trailing digit
        run into the number, and an Olympus/OM stem is P + month + day + frame
        ("P8081868" parses as prefix "P", number 8081868), so a same-day
        rollover reads as 8089999 followed by 8080001 - a decrease, which this
        rule rejects - and a rollover across midnight reads as 8089999 followed
        by 8090001, which this rule already allows.  Either way scan_stack_inputs
        walks strictly by ``number - 1``, so no stack can span the boundary
        regardless of what is merged here.  Loosening the range rule would only
        widen the window for adopting an unrelated frame.
        """
        relative_dir = output_record.get("relative_dir", ".")
        current_sequence = list(sequences_by_prefix.get((relative_dir, prefix), ()))

        previous_dir = None
        if scan_recursively:
            previous_dir = previous_adjacent_camera_dir(
                relative_dir, known_relative_dirs_by_key
            )
        previous_sequence = (
            list(sequences_by_prefix.get((previous_dir, prefix), ()))
            if previous_dir
            else []
        )

        if not previous_sequence:
            return current_sequence, [relative_dir] if current_sequence else []
        if not current_sequence:
            # Nothing in this folder to extend across the boundary.
            return [], []

        current_min = min(num for num, _ in current_sequence)
        previous_max = max(num for num, _ in previous_sequence)
        if previous_max >= current_min:
            if args.debug_stacks:
                print(
                    f"  - Adjacent folder '{previous_dir}' overlaps this folder's "
                    f"numbering (previous max {previous_max} >= current min "
                    f"{current_min}); it will not be used for stack inputs."
                )
            return current_sequence, [relative_dir]

        # Disjoint and strictly lower: a real boundary crossing.  Numbers
        # cannot collide, so no de-duplication is needed.
        sequence = sorted(previous_sequence + current_sequence)
        return sequence, [previous_dir, relative_dir]

    def stack_detection_group_key(record):
        """Group files for stack detection by folder and numeric filename prefix."""
        numeric_info = record.get("numeric")
        prefix = numeric_info["prefix"] if numeric_info else None
        return (record.get("relative_dir", "."), prefix)

    # Reliable stack detection needs RAW+JPG pairing; in groups (folder +
    # numeric filename prefix) that have JPGs but no RAW at all, disable it.
    # Only the Lightroom modes consume this, so skip the scan otherwise.
    jpg_only_stack_groups = set()
    metadata_positive_stack_groups = set()
    if operation_mode in ("lightroomimport", "lightroom"):
        groups_with_jpg = set()
        groups_with_raw = set()
        for stem, record in file_db.items():
            if stem in ambiguous_stems:
                continue
            has_jpg = record.get("has_jpg")
            # An .ORI companion still means the folder carries original files,
            # so it keeps the "JPG-only import" guard from firing spuriously.
            has_raw = has_raw_like_companion(record)
            if not (has_jpg or has_raw):
                continue
            group = stack_detection_group_key(record)
            if has_jpg:
                groups_with_jpg.add(group)
                jpg_record = record["files"].get("jpg")
                metadata = (
                    stack_metadata_by_path.get(jpg_record["path"])
                    if jpg_record
                    else None
                )
                if metadata and metadata.state == StackMetadataState.FOCUS_STACK:
                    metadata_positive_stack_groups.add(group)
            if has_raw:
                groups_with_raw.add(group)
        for group in groups_with_jpg - groups_with_raw:
            relative_dir, prefix = group
            previous_dir = (
                previous_adjacent_camera_dir(relative_dir, known_relative_dirs_by_key)
                if scan_recursively
                else None
            )
            if previous_dir and (previous_dir, prefix) in groups_with_raw:
                continue
            jpg_only_stack_groups.add(group)
    warned_jpg_only_stack_groups = set()

    def describe_stack_detection_group(group):
        relative_dir, prefix = group
        folder = src_dir if relative_dir == "." else os.path.join(src_dir, relative_dir)
        if prefix is None:
            return display_path(folder)
        return f"{display_path(folder)} (filename prefix '{prefix}')"

    def warn_jpg_only_stack_group(group):
        if group in warned_jpg_only_stack_groups:
            return
        warned_jpg_only_stack_groups.add(group)
        print(
            f"Warning: JPG-only import detected in {describe_stack_detection_group(group)}: "
            "no RAW files were found, so Stackcopy cannot reliably distinguish "
            "in-camera stack outputs from normal JPGs or focus-bracketing bursts. "
            "Stack detection has been disabled for this folder. All JPGs will be "
            "imported normally. For automatic stack sorting, enable RAW+JPG in "
            "the camera."
        )

    warned_inputs_not_all_raw_backed_dirs = set()

    def describe_stack_detection_folder(record):
        relative_dir = record.get("relative_dir", ".")
        folder = src_dir if relative_dir == "." else os.path.join(src_dir, relative_dir)
        return display_path(folder)

    def warn_inputs_not_all_raw_backed(record, input_stems):
        relative_dir = record.get("relative_dir", ".")
        if relative_dir in warned_inputs_not_all_raw_backed_dirs:
            return
        raw_backed_count = sum(
            has_standard_raw(file_db[input_stem]) for input_stem in input_stems
        )
        if raw_backed_count == 0:
            # Only the folder-level advice is once-per-folder.  A mixed set
            # says nothing about RAW+JPG being off, so it must not consume
            # the warning a later all-JPG stack in the same folder needs.
            warned_inputs_not_all_raw_backed_dirs.add(relative_dir)
            print(
                "Stack detection skipped in "
                f"{describe_stack_detection_folder(record)}: none of the inferred "
                "input frames are RAW-backed. Enable RAW+JPG for automatic stack "
                "sorting."
            )
        elif args.debug_stacks:
            print(
                "Stack detection skipped: inferred inputs are a mixed set "
                f"({raw_backed_count} RAW-backed of {len(input_stems)})."
            )

    def inferred_inputs_are_raw_backed(input_stems):
        """Heuristic stacks require *ordinary* RAW backing on every input.

        ``.ORI`` companions are excluded on purpose: an ORI-only sequence must
        not become more likely to be classified as a focus stack.
        """
        return bool(input_stems) and all(
            has_standard_raw(file_db[input_stem]) for input_stem in input_stems
        )

    def stack_metadata_for_record(record) -> StackMetadata:
        jpg_record = record["files"].get("jpg")
        if not jpg_record:
            return StackMetadata(StackMetadataState.UNKNOWN)
        return stack_metadata_by_path.get(
            jpg_record["path"], StackMetadata(StackMetadataState.UNKNOWN)
        )

    def find_stack_output_candidates():
        """Return metadata-confirmed or heuristic stack-output candidates."""
        if args.no_stack_detection:
            if args.debug_stacks:
                print("Stack detection disabled by --no-stack-detection.")
            return set()

        stacked_outputs = set()
        for stem, data in file_db.items():
            if stem in ambiguous_stems:
                continue
            if not data.get("has_jpg"):
                continue

            jpg_record = data["files"].get("jpg")
            if not jpg_record:
                continue

            if target_date:
                file_date = get_file_date(jpg_record, args.verbose)
                if file_date is None or file_date != target_date:
                    continue

            metadata = stack_metadata_for_record(data)
            if metadata.state == StackMetadataState.NOT_FOCUS_STACK:
                continue
            if metadata.state == StackMetadataState.FOCUS_STACK:
                stacked_outputs.add(stem)
                continue

            # Unknown metadata retains the established JPG-without-RAW
            # heuristic, including its JPG-only group safety guard.  Any
            # RAW-like companion (RAW or ORI) disqualifies the JPG as an
            # unpaired in-camera output.
            if has_raw_like_companion(data):
                continue
            group = stack_detection_group_key(data)
            if group in jpg_only_stack_groups:
                if group not in metadata_positive_stack_groups:
                    warn_jpg_only_stack_group(group)
                continue

            stacked_outputs.add(stem)
        return stacked_outputs

    def scan_stack_inputs(
        sequence, idx, output_num, output_mtime, output_data, claimed_input_stems
    ):
        """Backward-scan from a stacked-output candidate to collect its inputs.

        Walks the numeric sequence backwards from ``idx`` collecting contiguous,
        time-adjacent, RAW-backed frames as stack inputs, applies the burst-safety
        guard, and reports whether the inferred inputs are all RAW-backed. Shared
        by the --lightroomimport and --lightroom stack-detection passes.

        ``output_mtime`` must be non-None. Returns
        ``(potential_inputs, too_many_in_burst, inputs_not_all_raw_backed)``.
        """
        nonlocal inputs_not_all_raw_backed_skipped

        potential_inputs = []
        expected_num = output_num - 1
        current_index = idx - 1
        hit_input_cap = False
        stop_reason = "None"

        if args.debug_stacks:
            print("  - Scanning for Input frames (backward from output number):")

        prev_mtime = output_mtime
        allowed_gap = MAX_OUTPUT_LAG_SECONDS
        gap_type = "output_lag"

        while current_index >= 0 and len(potential_inputs) < MAX_STACK_INPUT_FRAMES:
            candidate_num, candidate_stem = sequence[current_index]
            candidate_record = file_db[candidate_stem]

            if candidate_num != expected_num:
                stop_reason = (
                    f"Number mismatch (expected {expected_num}, found {candidate_num})"
                )
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                break

            if candidate_stem in claimed_input_stems:
                stop_reason = "Already claimed by another stack"
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                break

            if not (
                has_raw_like_companion(candidate_record)
                or candidate_record.get("has_jpg")
            ):
                stop_reason = "No corresponding RAW or JPG file found"
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                break

            if (
                not has_standard_raw(candidate_record)
                and len(potential_inputs) >= MIN_STACK_INPUT_FRAMES
            ):
                stop_reason = "Non-RAW-backed boundary after sufficient inputs"
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': STOPPED ({stop_reason})")
                break

            input_mtime = get_stem_mtime(candidate_record, args.verbose)

            has_valid_raw_mtime = False
            raw_file_record = candidate_record["files"].get("raw")
            if raw_file_record:
                raw_mtime_val = get_file_mtime(raw_file_record, False)
                if raw_mtime_val:
                    has_valid_raw_mtime = True

            mtime_source = "RAW" if has_valid_raw_mtime else "JPG"

            if not input_mtime or not prev_mtime:
                time_gap = float("inf")
            else:
                time_gap = abs((prev_mtime - input_mtime).total_seconds())

            if time_gap > allowed_gap:
                stop_reason = f"Time gap too large ({time_gap:.2f}s > {allowed_gap}s, type: {gap_type})"
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                break

            if args.debug_stacks:
                print(
                    f"    - Input '{candidate_stem}': ACCEPTED (mtime source: {mtime_source}, {gap_type} gap={time_gap:.2f}s <= {allowed_gap}s)"
                )

            prev_mtime = input_mtime
            allowed_gap = MAX_INPUT_GAP_SECONDS
            gap_type = "input_gap"

            potential_inputs.append(candidate_stem)
            expected_num -= 1
            current_index -= 1

        if len(potential_inputs) == MAX_STACK_INPUT_FRAMES:
            hit_input_cap = True
            if args.debug_stacks:
                print(f"  - Note: Reached {MAX_STACK_INPUT_FRAMES}-frame input cap.")

        # A JPEG-only output plus consecutive RAW-backed preceding frames is
        # the primary stack evidence. Following photos are not part of this
        # candidate and are deliberately ignored. Only probe farther backward
        # after hitting the camera's maximum input count, to catch an apparent
        # continuous sequence extending beyond that supported stack size.
        too_many_in_burst = False
        if potential_inputs and hit_input_cap:
            burst_probe_stems = collect_consecutive_probe_stems(
                sequence,
                start_index=current_index,
                expected_num=expected_num,
                direction=-1,
                required_count=BURST_EXTRA_FRAMES_REQUIRED,
            )

            if len(burst_probe_stems) >= BURST_EXTRA_FRAMES_REQUIRED:
                first_input_stem = potential_inputs[-1]
                first_input_mtime = get_stem_mtime(
                    file_db[first_input_stem], args.verbose
                )

                all_in_burst_gap = True
                for probe_stem in burst_probe_stems:
                    probe_mtime = get_stem_mtime(file_db[probe_stem], args.verbose)
                    if not first_input_mtime or not probe_mtime:
                        all_in_burst_gap = False
                        break
                    gap = abs((first_input_mtime - probe_mtime).total_seconds())
                    if gap > MAX_BURST_GAP_SECONDS:
                        all_in_burst_gap = False
                        break

                if all_in_burst_gap:
                    too_many_in_burst = True
                    if args.debug_stacks:
                        print(
                            f"  - Burst Safety Check: TRIGGERED. Found {len(burst_probe_stems)} extra frames within {MAX_BURST_GAP_SECONDS}s of start."
                        )

        input_frames_are_raw_backed = inferred_inputs_are_raw_backed(potential_inputs)
        inputs_not_all_raw_backed = bool(potential_inputs) and not (
            input_frames_are_raw_backed
        )
        if inputs_not_all_raw_backed:
            inputs_not_all_raw_backed_skipped += 1
            warn_inputs_not_all_raw_backed(output_data, potential_inputs)
            if args.debug_stacks:
                print(
                    "  - Stack REJECTED: inferred input frames are not all RAW-backed; automatic stack detection requires RAW-backed input frames."
                )

        return potential_inputs, too_many_in_burst, inputs_not_all_raw_backed

    def scan_metadata_stack_inputs(
        sequence,
        idx,
        output_num,
        expected_count,
        output_mtime,
        claimed_input_stems,
        output_relative_dir=".",
    ):
        """Locate exactly N preceding components for a confirmed stack output.

        Camera metadata proves the output category. Numeric order localizes the
        available components; timestamps are diagnostics only and never revoke
        the camera's positive signal.
        """
        potential_inputs = []
        expected_num = output_num - 1
        current_index = idx - 1
        previous_mtime = output_mtime

        while current_index >= 0 and len(potential_inputs) < expected_count:
            candidate_num, candidate_stem = sequence[current_index]
            if candidate_num != expected_num or candidate_stem in claimed_input_stems:
                break
            candidate_record = file_db[candidate_stem]
            # Metadata is authoritative: a JPG+ORI component is a valid stack
            # input even though it has no ordinary RAW backing.
            if not (
                has_raw_like_companion(candidate_record)
                or candidate_record.get("has_jpg")
            ):
                break
            if (
                candidate_record.get("has_jpg")
                and stack_metadata_for_record(candidate_record).state
                == StackMetadataState.FOCUS_STACK
            ):
                break

            candidate_mtime = get_stem_mtime(candidate_record, args.verbose)
            candidate_dir = candidate_record.get("relative_dir", ".")
            if candidate_dir != output_relative_dir:
                # Folder provenance already limits which neighbouring frames
                # can be reached at all (see get_stack_sequence).  This is the
                # secondary sanity boundary: metadata proves this JPG is an
                # N-frame stack, but it proves nothing about a frame sitting in
                # a different folder, so an implausible capture gap - or an
                # unreadable timestamp on either side - disqualifies it.
                gap = None
                if previous_mtime and candidate_mtime:
                    gap = abs((previous_mtime - candidate_mtime).total_seconds())
                if gap is None or gap > MAX_CROSS_FOLDER_INPUT_GAP_SECONDS:
                    described_gap = (
                        f"{gap:.2f}s apart"
                        if gap is not None
                        else "no usable timestamp"
                    )
                    print(
                        f"Warning: not treating '{candidate_stem}' from the adjacent "
                        f"camera folder as a stack input ({described_gap}); it will be "
                        "imported as an ordinary photo."
                    )
                    break
                if args.debug_stacks:
                    print(
                        f"    - Metadata input '{candidate_stem}': accepted across the "
                        f"folder boundary ({gap:.2f}s gap)."
                    )
            elif args.debug_stacks and previous_mtime and candidate_mtime:
                gap = abs((previous_mtime - candidate_mtime).total_seconds())
                if gap > (
                    MAX_OUTPUT_LAG_SECONDS
                    if not potential_inputs
                    else MAX_INPUT_GAP_SECONDS
                ):
                    print(
                        f"    - Metadata input '{candidate_stem}': timestamp gap "
                        f"{gap:.2f}s is unusual; retained because StackedImage confirms the stack."
                    )
            potential_inputs.append(candidate_stem)
            previous_mtime = candidate_mtime or previous_mtime
            expected_num -= 1
            current_index -= 1

        missing_count = expected_count - len(potential_inputs)
        return potential_inputs, missing_count

    # --- 2. Process files based on operation mode ---

    if operation_mode == "lightroomimport":
        # ================================================================
        # --- Lightroom Import: Plan-Then-Execute Flow ---
        # ================================================================
        # Phase A: Detection and planning
        # Phase B: Disk space preflight
        # Phase C: Sort by mtime ascending
        # Phase D: Print summary
        # Phase E: Interactive confirmation (if requested)
        # Phase F: Execute moves sequentially
        # ================================================================

        collision_notified = set()
        reserved_dest_paths: set[str] = set()
        lightroom_import_base_dir = _lightroom_import_base_dir()

        # --- Phase A: Detection and Planning ---

        # A1. Find stacked output candidates (JPG without RAW)
        stacked_outputs = find_stack_output_candidates()

        # A2. Stack detection (same reverse-sorted walk as before)
        claimed_input_stems = set()
        processed_files_for_remaining: set[tuple[str, str]] = set()
        stack_output_names: dict[str, str] = {}

        planned_moves: list[PlannedMove] = []
        destination_checks: dict[tuple[str, str], DestinationCheck] = {}
        accepted_stacks = 0
        rejected_no_numeric_stem = 0
        rejected_no_sequence = 0
        rejected_first_in_sequence = 0
        rejected_no_mtime = 0
        rejected_too_few_inputs = 0
        rejected_burst_safety = 0
        skipped_claimed_as_input = 0
        skipped_missing_date = 0
        skipped_missing_at_plan = 0

        for output_stem in sorted(stacked_outputs, reverse=True):
            if output_stem in claimed_input_stems:
                # An earlier stack already took this frame as one of its
                # inputs; planning it as an output too would queue two moves
                # for the same source file.
                skipped_claimed_as_input += 1
                continue

            output_data = file_db[output_stem]
            jpg_record = output_data["files"].get("jpg")
            if not jpg_record:
                continue

            stack_outputs_seen += 1

            orig_jpg_path = jpg_record["path"]
            output_mtime = get_file_mtime(jpg_record, args.verbose)
            output_filename = os.path.basename(orig_jpg_path)

            if args.debug_stacks:
                print(f"\n--- Debugging Stack for Output: {output_filename} ---")
                print(f"  - Output JPG: '{output_filename}' (mtime: {output_mtime})")

            metadata = stack_metadata_for_record(output_data)
            metadata_confirmed = metadata.state == StackMetadataState.FOCUS_STACK
            numeric_info = output_data.get("numeric")
            sequence = []
            sequence_dirs = []
            potential_inputs = []
            too_many_in_burst = False
            inputs_not_all_raw_backed = False
            missing_metadata_inputs = 0

            if metadata_confirmed:
                expected_count = metadata.frame_count or 0
                if numeric_info:
                    prefix = numeric_info["prefix"]
                    output_num = numeric_info["num"]
                    sequence, sequence_dirs = get_stack_sequence(output_data, prefix)
                    idx = bisect_left(sequence, (output_num, ""))
                    potential_inputs, missing_metadata_inputs = (
                        scan_metadata_stack_inputs(
                            sequence,
                            idx,
                            output_num,
                            expected_count,
                            output_mtime,
                            claimed_input_stems,
                            output_data.get("relative_dir", "."),
                        )
                    )
                else:
                    missing_metadata_inputs = expected_count
                is_valid_stack = True
                if args.debug_stacks:
                    print(
                        f"  - Olympus StackedImage confirms a {expected_count}-frame focus stack."
                    )
                if missing_metadata_inputs:
                    print(
                        f"Warning: camera metadata confirms '{output_filename}' is a "
                        f"{expected_count}-frame focus stack, but "
                        f"{missing_metadata_inputs} expected source frame(s) are unavailable. "
                        "The confirmed output and available consecutive inputs will still be sorted."
                    )
            else:
                if not numeric_info:
                    if args.debug_stacks:
                        print("  - Stack REJECTED: Output JPG has no numeric stem.")
                    rejected_no_numeric_stem += 1
                    continue

                prefix = numeric_info["prefix"]
                output_num = numeric_info["num"]
                sequence, sequence_dirs = get_stack_sequence(output_data, prefix)
                if not sequence:
                    if args.debug_stacks:
                        print("  - Stack REJECTED: No sequence found for this prefix.")
                    rejected_no_sequence += 1
                    continue
                idx = bisect_left(sequence, (output_num, ""))
                if idx == 0:
                    if args.debug_stacks:
                        print(
                            "  - Stack REJECTED: Output is the first in its sequence."
                        )
                    rejected_first_in_sequence += 1
                    continue
                if output_mtime is None:
                    if args.debug_stacks:
                        print("  - Stack REJECTED: Output mtime is missing.")
                    rejected_no_mtime += 1
                    continue
                potential_inputs, too_many_in_burst, inputs_not_all_raw_backed = (
                    scan_stack_inputs(
                        sequence,
                        idx,
                        output_num,
                        output_mtime,
                        output_data,
                        claimed_input_stems,
                    )
                )
                is_valid_stack = (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                    and not too_many_in_burst
                    and not inputs_not_all_raw_backed
                )

            if args.debug_stacks and numeric_info:
                print(f"  - Numeric Stem Info: prefix='{prefix}', number={output_num}")
                if len(sequence_dirs) > 1:
                    print(
                        "  - Sequence includes adjacent camera folders: "
                        + ", ".join(sequence_dirs)
                    )

            if args.debug_stacks:
                print(
                    f"  - Final Decision: {'ACCEPTED' if is_valid_stack else 'REJECTED'}"
                )
                if not metadata_confirmed and not (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                ):
                    print(
                        f"    - Reason: Found {len(potential_inputs)} inputs, but requires "
                        f"{MIN_STACK_INPUT_FRAMES}-{MAX_STACK_INPUT_FRAMES}."
                    )
                if not metadata_confirmed and too_many_in_burst:
                    print(
                        "    - Reason: Burst safety check failed (likely a focus bracket)."
                    )
                if not metadata_confirmed and inputs_not_all_raw_backed:
                    print("    - Reason: Inferred input frames are not all RAW-backed.")
                print("--- End Debugging Stack ---")

            if not is_valid_stack:
                if (
                    not metadata_confirmed
                    and not inputs_not_all_raw_backed
                    and not (
                        MIN_STACK_INPUT_FRAMES
                        <= len(potential_inputs)
                        <= MAX_STACK_INPUT_FRAMES
                    )
                ):
                    rejected_too_few_inputs += 1
                if too_many_in_burst:
                    rejected_burst_safety += 1
                continue

            # --- Stack accepted: plan moves ---
            accepted_stacks += 1
            stack_group_of_stem[output_stem] = output_stem
            for input_stem in potential_inputs:
                claimed_input_stems.add(input_stem)
                stack_group_of_stem[input_stem] = output_stem

            # Plan the stacked output move: source -> ~/Pictures/Lightroom/YEAR/DATE/
            # with "stacked" suffix applied to the destination filename
            file_date = get_file_date(jpg_record, args.verbose)
            if file_date:
                dest_dir_import = os.path.join(
                    lightroom_import_base_dir,
                    str(file_date.year),
                    file_date.strftime("%Y-%m-%d"),
                )

                # Build the destination filename with "stacked" suffix
                if not is_already_processed(output_filename):
                    stem_only, ext = os.path.splitext(output_filename)
                    dest_basename = create_new_filename(stem_only, ext, args.prefix)
                else:
                    dest_basename = output_filename

                # Collision-safe naming against the final destination
                out_files = {"jpg": {"basename": dest_basename, "path": orig_jpg_path}}
                try:
                    counter, chosen = pick_unique_basenames_for_stem(
                        dest_dir_import,
                        out_files,
                        args.force,
                        args.dry_run,
                        reserved_paths=reserved_dest_paths,
                        destination_checks=destination_checks,
                    )
                except DestinationNameExhausted as exhausted:
                    name_exhausted_count += 1
                    print(
                        f"Error: cannot import stacked output '{output_filename}' "
                        f"(stem '{output_stem}'): {exhausted}."
                    )
                    print(
                        "  The whole stack is left on the source so it stays "
                        "together; nothing was overwritten."
                    )
                    # Keep the stack intact on the card: skip its output *and*
                    # its inputs rather than importing half of it.
                    processed_files_for_remaining.add((output_stem, "jpg"))
                    for input_stem in potential_inputs:
                        for input_file_type in REMAINING_FILE_TYPES:
                            processed_files_for_remaining.add(
                                (input_stem, input_file_type)
                            )
                    continue
                chosen_name = chosen.get("jpg", dest_basename)
                if counter > 1:
                    key = (dest_dir_import, output_filename)
                    if key not in collision_notified:
                        collision_notified.add(key)
                        print_collision_rename_notice(
                            dest_dir_import,
                            output_filename,
                            [(dest_basename, chosen_name)],
                            args.dry_run,
                        )

                dest_path = os.path.join(dest_dir_import, chosen_name)
                reserved_dest_paths.add(reservation_key(dest_path))
                planned_moves.append(
                    PlannedMove(
                        src_path=orig_jpg_path,
                        dest_path=dest_path,
                        category="stack_output",
                        stem=output_stem,
                        file_type="jpg",
                        mtime=output_mtime,
                        basename_orig=output_filename,
                        basename_dest=chosen_name,
                        dest_dir=dest_dir_import,
                        stack_output_name=chosen_name,
                        destination_check=destination_checks.get(
                            (orig_jpg_path, dest_path)
                        ),
                    )
                )
                stack_output_names[output_stem] = chosen_name
                processed_files_for_remaining.add((output_stem, "jpg"))
            else:
                skipped_missing_date += 1
                if args.verbose:
                    print(
                        f"Warning: Could not determine date for stacked output '{output_filename}', skipping."
                    )

            # Plan input file moves
            for input_stem in potential_inputs:
                raw_record = file_db[input_stem]["files"].get("raw")
                date_record = (
                    raw_record
                    or file_db[input_stem]["files"].get("ori")
                    or file_db[input_stem]["files"].get("jpg")
                )
                if not date_record:
                    continue

                input_file_date = get_file_date(date_record, args.verbose)
                if not input_file_date:
                    skipped_missing_date += 1
                    if args.verbose:
                        print(
                            f"Warning: Could not determine date for '{input_stem}', skipping move."
                        )
                    continue

                lightroom_dest_dir = os.path.join(
                    STACK_INPUT_DIR,
                    str(input_file_date.year),
                    input_file_date.strftime("%Y-%m-%d"),
                )

                # Collision-safe naming for the stem's files
                stem_files = {
                    "jpg": file_db[input_stem]["files"].get("jpg"),
                    "raw": file_db[input_stem]["files"].get("raw"),
                    "ori": file_db[input_stem]["files"].get("ori"),
                }
                stem_files = {k: v for k, v in stem_files.items() if v}
                try:
                    counter, chosen = pick_unique_basenames_for_stem(
                        lightroom_dest_dir,
                        stem_files,
                        args.force,
                        args.dry_run,
                        reserved_paths=reserved_dest_paths,
                        destination_checks=destination_checks,
                    )
                except DestinationNameExhausted as exhausted:
                    name_exhausted_count += len(stem_files)
                    print(
                        f"Error: cannot move stack input '{input_stem}': "
                        f"{exhausted}. Its files are left on the source; "
                        "nothing was overwritten."
                    )
                    for input_file_type in REMAINING_FILE_TYPES:
                        processed_files_for_remaining.add((input_stem, input_file_type))
                    continue
                if counter > 1:
                    changes = []
                    for ft, fi in stem_files.items():
                        old = fi["basename"]
                        new = chosen.get(ft, old)
                        if new != old:
                            changes.append((old, new))

                    key = (lightroom_dest_dir, input_stem)
                    if key not in collision_notified:
                        collision_notified.add(key)
                        print_collision_rename_notice(
                            lightroom_dest_dir,
                            input_stem,
                            changes,
                            args.dry_run,
                        )

                for file_type in ["jpg", "raw", "ori"]:
                    file_info = file_db[input_stem]["files"].get(file_type)
                    if not file_info:
                        continue
                    src_path = file_info["path"]
                    if not os.path.exists(src_path):
                        skipped_missing_at_plan += 1
                        if args.verbose:
                            print(
                                f"Warning: File '{src_path}' missing at plan time, skipping."
                            )
                        continue

                    input_mtime_val = get_file_mtime(file_info, args.verbose)
                    chosen_basename = chosen.get(file_type, file_info["basename"])
                    dest_path = os.path.join(lightroom_dest_dir, chosen_basename)
                    reserved_dest_paths.add(reservation_key(dest_path))
                    planned_moves.append(
                        PlannedMove(
                            src_path=src_path,
                            dest_path=dest_path,
                            category="stack_input",
                            stem=input_stem,
                            file_type=file_type,
                            mtime=input_mtime_val,
                            basename_orig=file_info["basename"],
                            basename_dest=chosen_basename,
                            dest_dir=lightroom_dest_dir,
                            stack_output_name=stack_output_names.get(output_stem),
                            destination_check=destination_checks.get(
                                (src_path, dest_path)
                            ),
                        )
                    )
                    processed_files_for_remaining.add((input_stem, file_type))

        # A3. Plan remaining files
        for stem, record in file_db.items():
            if stem in ambiguous_stems:
                continue
            files_by_dest: dict[str, list[tuple[str, dict]]] = defaultdict(list)

            # One date for the whole stem's photo companions.  Their
            # filesystem mtimes can differ by a fraction of a second, and if
            # that straddles midnight the JPG, RAW and ORI of a single frame
            # would land in two adjacent date folders.  RAW wins, then ORI,
            # then JPG - the same order of authority get_stem_mtime and the
            # stack-input planner already use.  Videos are independent
            # recordings and keep their own date.
            companion_date = None
            for date_source_type in ("raw", "ori", "jpg"):
                if (stem, date_source_type) in processed_files_for_remaining:
                    continue
                date_source = record["files"].get(date_source_type)
                if not date_source or not os.path.exists(date_source["path"]):
                    continue
                companion_date = get_file_date(date_source, args.verbose)
                if companion_date is not None:
                    break

            for file_type in REMAINING_FILE_TYPES:
                if (stem, file_type) in processed_files_for_remaining:
                    continue

                file_info = record["files"].get(file_type)
                if not file_info:
                    continue

                src_path = file_info["path"]
                if not os.path.exists(src_path):
                    skipped_missing_at_plan += 1
                    continue

                if file_type == "video":
                    file_date = get_file_date(file_info, args.verbose)
                else:
                    file_date = companion_date
                if file_date is None:
                    skipped_missing_date += 1
                    if args.verbose:
                        print(
                            f"Warning: Could not determine date for '{src_path}', skipping import move."
                        )
                    continue
                if target_date and file_date != target_date:
                    continue

                dest_dir_import = os.path.join(
                    lightroom_import_base_dir,
                    str(file_date.year),
                    file_date.strftime("%Y-%m-%d"),
                )
                files_by_dest[dest_dir_import].append((file_type, file_info))

            for dest_dir_import, files in files_by_dest.items():
                stem_files_for_dest = dict(files)
                try:
                    counter, chosen = pick_unique_basenames_for_stem(
                        dest_dir_import,
                        stem_files_for_dest,
                        args.force,
                        args.dry_run,
                        reserved_paths=reserved_dest_paths,
                        destination_checks=destination_checks,
                    )
                except DestinationNameExhausted as exhausted:
                    name_exhausted_count += len(stem_files_for_dest)
                    print(
                        f"Error: cannot import '{stem}': {exhausted}. "
                        "Its files are left on the source; nothing was overwritten."
                    )
                    continue
                if counter > 1:
                    changes = []
                    for ft, fi in stem_files_for_dest.items():
                        old = fi["basename"]
                        new = chosen.get(ft, old)
                        if new != old:
                            changes.append((old, new))

                    key = (dest_dir_import, stem)
                    if key not in collision_notified:
                        collision_notified.add(key)
                        print_collision_rename_notice(
                            dest_dir_import, stem, changes, args.dry_run
                        )

                for ft, file_info in files:
                    file_mtime_val = get_file_mtime(file_info, args.verbose)
                    chosen_basename = chosen.get(ft, file_info["basename"])
                    dest_path = os.path.join(dest_dir_import, chosen_basename)
                    reserved_dest_paths.add(reservation_key(dest_path))
                    planned_moves.append(
                        PlannedMove(
                            src_path=file_info["path"],
                            dest_path=dest_path,
                            category="remaining",
                            stem=stem,
                            file_type=ft,
                            mtime=file_mtime_val,
                            basename_orig=file_info["basename"],
                            basename_dest=chosen_basename,
                            dest_dir=dest_dir_import,
                            destination_check=destination_checks.get(
                                (file_info["path"], dest_path)
                            ),
                        )
                    )

        file_operation = "copy" if args.leave_on_card else "move"
        operation_label = "copying" if args.leave_on_card else "moving"
        planned_action_noun = "copies" if args.leave_on_card else "moves"
        past_tense_verb = "Copied" if args.leave_on_card else "Moved"
        # "Would copy"/"Will copy" for the plan summary, "Would copy"/"Copied"
        # for per-file execution lines (and likewise for moves).
        planned_verb = (
            f"Would {file_operation}" if args.dry_run else f"Will {file_operation}"
        )
        done_verb = f"Would {file_operation}" if args.dry_run else past_tense_verb

        # --- Phase C: Sort by mtime ascending ---
        planned_moves.sort(
            key=lambda m: (m.mtime or datetime.min, m.basename_orig, m.src_path)
        )

        # --- Phase D: Print summary ---
        planned_output_count = sum(
            1 for m in planned_moves if m.category == "stack_output"
        )
        planned_input_count = sum(
            1 for m in planned_moves if m.category == "stack_input"
        )
        planned_remaining_count = sum(
            1 for m in planned_moves if m.category == "remaining"
        )
        total_rejected = stack_outputs_seen - accepted_stacks
        all_dest_dirs = sorted(set(display_path(m.dest_dir) for m in planned_moves))

        if args.plan_json:
            planned_bytes = 0
            for move in planned_moves:
                try:
                    planned_bytes += os.path.getsize(move.src_path)
                except OSError:
                    pass
            mtimes = [move.mtime for move in planned_moves if move.mtime is not None]
            newest = max(mtimes).date() if mtimes else None
            if newest is not None:
                newest_text = newest.isoformat()
                dated_lightroom = os.path.join(
                    lightroom_import_base_dir,
                    str(newest.year),
                    newest_text,
                )
                dated_stack_input = os.path.join(
                    STACK_INPUT_DIR,
                    str(newest.year),
                    newest_text,
                )
            else:
                newest_text = None
                dated_lightroom = lightroom_import_base_dir
                dated_stack_input = STACK_INPUT_DIR

            other_videos = sum(
                1
                for move in planned_moves
                if move.category == "remaining" and move.file_type == "video"
            )
            planned_sources = [move.src_path for move in planned_moves]
            would_be_empty = card_would_be_empty_after(src_dir, planned_sources)
            if args.leave_on_card and planned_sources:
                would_be_empty = False
            payload = {
                "total": len(planned_moves),
                "bytes": planned_bytes,
                "stacks": accepted_stacks,
                "stacked_outputs": planned_output_count,
                "stack_inputs": planned_input_count,
                "others": planned_remaining_count,
                "other_photos": planned_remaining_count - other_videos,
                "other_videos": other_videos,
                "newest_date": newest_text,
                "dest_lightroom": dated_lightroom,
                "dest_stack_input": dated_stack_input,
                "source_subdirs_scanned": sorted(scanned_source_subdirs),
                "source_is_removable": source_is_removable(src_dir),
                "source_would_be_empty_after": would_be_empty,
            }
            output_example = next(
                (
                    move.basename_dest
                    for move in planned_moves
                    if move.category == "stack_output"
                ),
                None,
            )
            if output_example is not None:
                payload["stacked_output_example"] = output_example
            assert plan_json_stdout is not None
            sys.stdout = plan_json_stdout
            print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
            return

        verb = planned_verb
        dry_prefix = "DRY RUN: " if args.dry_run else ""

        _emit_progress(phase="prepare", done=0, total=len(planned_moves))

        print(f"\n{dry_prefix}Planned Lightroom import for '{src_dir}':")
        print(f"  Stacked JPG candidates found:  {len(stacked_outputs)}")
        if skipped_claimed_as_input:
            print(f"  Skipped (claimed as input):    {skipped_claimed_as_input}")
        print(f"  Evaluated as potential stacks:  {stack_outputs_seen}")
        print(f"  Accepted stacks:               {accepted_stacks}")
        print(f"  Rejected stack candidates:     {total_rejected}")
        print(
            f"  Input sequences not all RAW-backed skipped: {inputs_not_all_raw_backed_skipped}"
        )

        if args.debug_stacks and total_rejected > 0:
            print("    Rejection breakdown:")
            if rejected_no_numeric_stem:
                print(f"      No numeric stem:           {rejected_no_numeric_stem}")
            if rejected_no_sequence:
                print(f"      No sequence found:         {rejected_no_sequence}")
            if rejected_first_in_sequence:
                print(f"      First in sequence:         {rejected_first_in_sequence}")
            if rejected_no_mtime:
                print(f"      Missing mtime:             {rejected_no_mtime}")
            if rejected_too_few_inputs:
                print(f"      Too few inputs:            {rejected_too_few_inputs}")
            if rejected_burst_safety:
                print(f"      Burst safety:              {rejected_burst_safety}")
            if inputs_not_all_raw_backed_skipped:
                print(
                    f"      Inputs not all RAW-backed: {inputs_not_all_raw_backed_skipped}"
                )

        print(f"  {verb} {planned_output_count} stacked output files")
        print(f"  {verb} {planned_input_count} stack input files")
        print(f"  {verb} {planned_remaining_count} remaining files")
        print(f"  Total planned {planned_action_noun}:           {len(planned_moves)}")

        if skipped_missing_date or skipped_missing_at_plan:
            print()
            if skipped_missing_date:
                print(f"  Skipped (no date available):   {skipped_missing_date}")
            if skipped_missing_at_plan:
                print(f"  Skipped (file missing):        {skipped_missing_at_plan}")

        if planned_moves:
            mtimes = [m.mtime for m in planned_moves if m.mtime is not None]
            if mtimes:
                earliest = min(mtimes)
                latest = max(mtimes)
                print("\n  Time range:")
                print(f"    Earliest: {earliest.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"    Latest:   {latest.strftime('%Y-%m-%d %H:%M:%S')}")

            if all_dest_dirs:
                print("\n  Destinations:")
                for d in all_dest_dirs:
                    print(f"    {d}")

        print()

        # Disk-space warnings follow the plan so users know what they are
        # being asked to approve. Dry runs report low space but never prompt.
        if planned_moves:
            ops_for_check = [
                (m.src_path, m.dest_path, file_operation)
                for m in planned_moves
                if not (
                    m.destination_check
                    and m.destination_check.state == DestinationState.IDENTICAL
                )
            ]
            if ops_for_check:
                confirm_if_low_space(ops_for_check, args.dry_run)

        # --- Phase E: Interactive confirmation ---
        if args.interactive and not args.dry_run:
            if not sys.stdin.isatty():
                print("Error: --interactive requires a terminal (TTY) for input.")
                sys.exit(1)
            while True:
                response = input("Continue? Type y or n: ").strip().lower()
                if response == "y":
                    break
                elif response == "n":
                    print("Aborted.")
                    sys.exit(0)
                else:
                    print("Please type y or n.")

        # --- Phase F: Execute file operations sequentially ---
        exec_start_time = time.perf_counter()
        _emit_progress(phase="start", done=0, total=len(planned_moves))
        # execution_results already initialized at top of main
        for move in planned_moves:
            if move.stem not in execution_results:
                execution_results[move.stem] = {
                    "expected": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "stack_expected": 0,
                    "stack_succeeded": 0,
                    "moves": [],
                }
            execution_results[move.stem]["expected"] += 1
            if move.category != "remaining":
                execution_results[move.stem]["stack_expected"] += 1

        _total_planned = len(planned_moves)
        attempted_count = 0
        try:
            for _move_index, move in enumerate(planned_moves):
                progress_role = (
                    "other" if move.category == "remaining" else move.category
                )
                progress_fields: dict[str, Any] = {
                    "phase": file_operation,
                    "done": _move_index,
                    "total": _total_planned,
                    "role": progress_role,
                }
                if move.stack_output_name:
                    progress_fields["stack_output_name"] = quote(
                        move.stack_output_name, safe=""
                    )
                _emit_progress(
                    file=os.path.basename(move.src_path),
                    **progress_fields,
                )
                try:
                    ensure_directory_once(move.dest_dir, created_dirs, args.dry_run)
                except OSError as directory_error:
                    print(
                        f"Error creating destination directory '{move.dest_dir}' for "
                        f"'{move.basename_orig}': {directory_error}"
                    )
                    success, bytes_moved = False, 0
                else:
                    success, bytes_moved = safe_file_operation(
                        file_operation,
                        move.src_path,
                        move.dest_path,
                        f"{operation_label} {move.category.replace('_', ' ')} file",
                        args.force,
                        args.dry_run,
                        move.destination_check,
                    )

                attempted_count += 1
                outcome = operation_outcome_of(success)
                res = execution_results[move.stem]
                if not args.dry_run:
                    if success:
                        # A copy whose source could not be removed still put
                        # the file at its destination, so it counts as placed:
                        # recovery must not run and produce a duplicate.
                        res["succeeded"] += 1
                        if move.category != "remaining":
                            res["stack_succeeded"] += 1
                    else:
                        res["failed"] += 1
                        primary_failure_count += 1
                res["moves"].append(
                    {
                        "move": move,
                        "success": bool(success),
                        "outcome": outcome,
                        "recovered": False,
                    }
                )

                if success:
                    total_bytes_moved += bytes_moved
                    # A file whose source could not be removed is placed but
                    # not moved.  It is reported on its own line rather than
                    # inflating the per-category move counts.
                    if outcome is not OperationOutcome.COPIED_SOURCE_REMAINS:
                        if move.category == "stack_output":
                            moved_output_count += 1
                            processed_count += 1
                            moved_stack_groups.add(
                                stack_group_of_stem.get(move.stem, move.stem)
                            )
                        elif move.category == "stack_input":
                            moved_input_count += 1
                            moved_stack_groups.add(
                                stack_group_of_stem.get(move.stem, move.stem)
                            )
                        elif move.category == "remaining":
                            remaining_moved_count += 1

                    if args.verbose or args.dry_run:
                        verb = done_verb
                        dest_short = display_path(move.dest_dir)
                        if move.basename_dest != move.basename_orig:
                            print(
                                f"{verb} {move.category.replace('_', ' ')} '{move.basename_orig}' as '{move.basename_dest}' -> '{dest_short}'"
                            )
                        else:
                            print(
                                f"{verb} {move.category.replace('_', ' ')} '{move.basename_orig}' -> '{dest_short}'"
                            )
        except KeyboardInterrupt:
            interrupted = True
            interrupted_remaining = _total_planned - attempted_count
            print(
                "\nImport interrupted by user. No new operations will be started; "
                "the import can be safely run again."
            )

        # --- Phase G: Recovery Pass & Partial Failure Reporting ---
        # 1. Detect stems that failed COMPLETELY and still need to be "recovered"
        # 2. Detect and report "Partial Failures" (some moved, some didn't)
        recovery_stems: set[str] = set()
        partial_failures_found = False

        for stem, res in execution_results.items():
            expected = res["expected"]
            succeeded = res["succeeded"]
            failed = res["failed"]
            stack_expected = res["stack_expected"]
            stack_succeeded = res["stack_succeeded"]

            if not args.dry_run and failed > 0:
                # RECOVERY LOGIC: If it has stack-planned files and they ALL failed, recover the stem.
                if stack_expected > 0 and stack_succeeded == 0:
                    recovery_stems.add(stem)
                elif succeeded > 0:
                    # Generic partial failure reporting (some succeeded, some failed)
                    partial_failures_found = True
                    print(f"\n*** PARTIAL FAILURE WARNING for stem '{stem}' ***")
                    print(f"  Only {succeeded} of {expected} planned moves succeeded.")
                    for m_res in res["moves"]:
                        move = m_res["move"]
                        if (
                            m_res.get("outcome")
                            is OperationOutcome.COPIED_SOURCE_REMAINS
                        ):
                            status = "COPIED, SOURCE REMAINS"
                        else:
                            status = "SUCCESS" if m_res["success"] else "FAILED"
                        print(f"  [{status}] {move.basename_orig} -> {move.dest_path}")
                    print("*********************************************\n")

        if recovery_stems and not interrupted:
            recovery_reserved_paths = {
                reservation_key(move_result["move"].dest_path)
                for result in execution_results.values()
                for move_result in result["moves"]
                if move_result["success"]
            }
            if args.verbose:
                print(
                    f"\nRecovering {len(recovery_stems)} stem(s) whose planned moves all failed..."
                )
            for stem in recovery_stems:
                files_by_dest: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for move_result in execution_results[stem]["moves"]:
                    if move_result["success"]:
                        continue
                    move = move_result["move"]
                    if not os.path.exists(move.src_path) or move.mtime is None:
                        continue
                    file_date = move.mtime.date()
                    dest_dir_import = os.path.join(
                        lightroom_import_base_dir,
                        str(file_date.year),
                        file_date.strftime("%Y-%m-%d"),
                    )
                    files_by_dest[dest_dir_import].append(move_result)

                for dest_dir_import, move_results in files_by_dest.items():
                    try:
                        ensure_directory_once(
                            dest_dir_import, created_dirs, args.dry_run
                        )
                    except OSError as directory_error:
                        print(
                            f"Error creating recovery directory '{dest_dir_import}' "
                            f"for stem '{stem}': {directory_error}"
                        )
                        continue
                    stem_files_for_dest = {}
                    for move_result in move_results:
                        move = move_result["move"]
                        recovery_basename = move.basename_dest
                        stem_files_for_dest[move.file_type] = {
                            "basename": recovery_basename,
                            "path": move.src_path,
                        }
                    try:
                        counter, chosen = pick_unique_basenames_for_stem(
                            dest_dir_import,
                            stem_files_for_dest,
                            args.force,
                            args.dry_run,
                            reserved_paths=recovery_reserved_paths,
                        )
                    except DestinationNameExhausted as exhausted:
                        name_exhausted_count += len(stem_files_for_dest)
                        print(
                            f"Error: cannot recover stem '{stem}': {exhausted}. "
                            "Its files are left on the source; nothing was overwritten."
                        )
                        continue
                    if counter > 1:
                        changes = []
                        for ft, fi in stem_files_for_dest.items():
                            old = fi["basename"]
                            new = chosen.get(ft, old)
                            if new != old:
                                changes.append((old, new))
                        print_collision_rename_notice(
                            dest_dir_import, stem, changes, args.dry_run
                        )

                    for move_result in move_results:
                        move = move_result["move"]
                        file_info = stem_files_for_dest[move.file_type]
                        file_dest_basename = chosen.get(
                            move.file_type, file_info["basename"]
                        )
                        dest_path = os.path.join(dest_dir_import, file_dest_basename)
                        recovery_reserved_paths.add(reservation_key(dest_path))

                        success, bytes_moved = safe_file_operation(
                            file_operation,
                            move.src_path,
                            dest_path,
                            f"{operation_label} recovered {move.category.replace('_', ' ')} file",
                            args.force,
                            args.dry_run,
                        )
                        if success:
                            move_result["recovered"] = True
                            recovered_count += 1
                            if (
                                operation_outcome_of(success)
                                is OperationOutcome.COPIED_SOURCE_REMAINS
                            ):
                                recovered_source_remains_count += 1
                            recovery_dest_dirs.add(display_path(dest_dir_import))
                            total_bytes_moved += bytes_moved
                            if move.category != "remaining":
                                moved_stack_groups.add(
                                    stack_group_of_stem.get(move.stem, move.stem)
                                )
                            if args.verbose or args.dry_run:
                                verb = done_verb
                                dest_short = display_path(dest_dir_import)
                                print(
                                    f"{verb} recovered {move.category.replace('_', ' ')} '{move.basename_orig}' as '{file_dest_basename}' -> '{dest_short}'"
                                    if file_dest_basename != move.basename_orig
                                    else f"{verb} recovered {move.category.replace('_', ' ')} '{move.basename_orig}' -> '{dest_short}'"
                                )

        if not args.dry_run:
            failed_count = sum(
                1
                for result in execution_results.values()
                for move_result in result["moves"]
                if not move_result["success"] and not move_result["recovered"]
            )

        if exec_start_time is not None:
            exec_elapsed_time = time.perf_counter() - exec_start_time

        _emit_progress(
            phase="interrupted" if interrupted else "done",
            done=attempted_count,
            total=len(planned_moves),
            # Tells the GUI the import finished but not as planned, so it
            # never shows a plain "Import complete".
            degraded=1 if (recovered_count or source_remains_paths) else 0,
        )

    elif args.lightroom is not None:
        # ================================================================
        # --- Lightroom Mode (non-import): existing behavior unchanged ---
        # ================================================================
        input_dest_dirs = set()
        collision_notified = set()
        stacked_outputs = find_stack_output_candidates()

        claimed_input_stems = set()
        processed_stems_for_remaining = set()

        move_operations = []
        expected_moves_per_stem = defaultdict(int)
        successful_moves_per_stem = defaultdict(int)
        lightroom_moves_attempted = 0

        stack_output_order = sorted(stacked_outputs, reverse=True)
        for output_index, output_stem in enumerate(stack_output_order):
            if output_stem in claimed_input_stems:
                # An earlier stack already took this frame as one of its
                # inputs; treating it as an output too would rename a file
                # that is already queued to move somewhere else.
                continue

            output_data = file_db[output_stem]
            jpg_record = output_data["files"].get("jpg")
            if not jpg_record:
                continue

            stack_outputs_seen += 1

            orig_jpg_path = jpg_record["path"]
            output_mtime = get_file_mtime(jpg_record, args.verbose)
            output_filename = os.path.basename(orig_jpg_path)

            if args.debug_stacks:
                print(f"\n--- Debugging Stack for Output: {output_filename} ---")
                print(f"  - Output JPG: '{output_filename}' (mtime: {output_mtime})")

            metadata = stack_metadata_for_record(output_data)
            metadata_confirmed = metadata.state == StackMetadataState.FOCUS_STACK
            numeric_info = output_data.get("numeric")
            potential_inputs = []
            too_many_in_burst = False
            inputs_not_all_raw_backed = False

            if metadata_confirmed:
                expected_count = metadata.frame_count or 0
                if numeric_info:
                    prefix = numeric_info["prefix"]
                    output_num = numeric_info["num"]
                    sequence, sequence_dirs = get_stack_sequence(output_data, prefix)
                    idx = bisect_left(sequence, (output_num, ""))
                    potential_inputs, missing_metadata_inputs = (
                        scan_metadata_stack_inputs(
                            sequence,
                            idx,
                            output_num,
                            expected_count,
                            output_mtime,
                            claimed_input_stems,
                            output_data.get("relative_dir", "."),
                        )
                    )
                else:
                    missing_metadata_inputs = expected_count
                is_valid_stack = True
                if missing_metadata_inputs:
                    print(
                        f"Warning: camera metadata confirms '{output_filename}' is a "
                        f"{expected_count}-frame focus stack, but "
                        f"{missing_metadata_inputs} expected source frame(s) are unavailable. "
                        "The confirmed output and available consecutive inputs will still be sorted."
                    )
            else:
                if not numeric_info:
                    if args.debug_stacks:
                        print("  - Stack REJECTED: Output JPG has no numeric stem.")
                    continue
                prefix = numeric_info["prefix"]
                output_num = numeric_info["num"]
                sequence, sequence_dirs = get_stack_sequence(output_data, prefix)
                if not sequence:
                    continue
                idx = bisect_left(sequence, (output_num, ""))
                if idx == 0 or output_mtime is None:
                    continue
                potential_inputs, too_many_in_burst, inputs_not_all_raw_backed = (
                    scan_stack_inputs(
                        sequence,
                        idx,
                        output_num,
                        output_mtime,
                        output_data,
                        claimed_input_stems,
                    )
                )
                is_valid_stack = (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                    and not too_many_in_burst
                    and not inputs_not_all_raw_backed
                )

            if args.debug_stacks and numeric_info:
                print(f"  - Numeric Stem Info: prefix='{prefix}', number={output_num}")
                if len(sequence_dirs) > 1:
                    print(
                        "  - Sequence includes adjacent camera folders: "
                        + ", ".join(sequence_dirs)
                    )

            if args.debug_stacks:
                print(
                    f"  - Final Decision: {'ACCEPTED' if is_valid_stack else 'REJECTED'}"
                )
                if not metadata_confirmed and not (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                ):
                    print(
                        f"    - Reason: Found {len(potential_inputs)} inputs, but requires "
                        f"{MIN_STACK_INPUT_FRAMES}-{MAX_STACK_INPUT_FRAMES}."
                    )
                if not metadata_confirmed and too_many_in_burst:
                    print(
                        "    - Reason: Burst safety check failed (likely a focus bracket)."
                    )
                if not metadata_confirmed and inputs_not_all_raw_backed:
                    print("    - Reason: Inferred input frames are not all RAW-backed.")
                print("--- End Debugging Stack ---")

            if is_valid_stack:
                for input_stem in potential_inputs:
                    claimed_input_stems.add(input_stem)

                output_move_success = False

                if not is_already_processed(output_filename):
                    stem_only, ext = os.path.splitext(output_filename)
                    new_filename = create_new_filename(stem_only, ext, args.prefix)
                    out_files = {
                        "jpg": {"basename": new_filename, "path": orig_jpg_path}
                    }
                    try:
                        counter, chosen = pick_unique_basenames_for_stem(
                            dest_dir, out_files, args.force, args.dry_run
                        )
                    except DestinationNameExhausted as exhausted:
                        name_exhausted_count += 1
                        failed_count += 1
                        print(
                            f"Error: cannot rename stacked output "
                            f"'{output_filename}': {exhausted}. It is left in "
                            "place; nothing was overwritten."
                        )
                        continue
                    chosen_name = chosen.get("jpg", new_filename)
                    dest_path = os.path.join(dest_dir, chosen_name)
                    if counter > 1:
                        key = (dest_dir, output_filename)
                        if key not in collision_notified:
                            collision_notified.add(key)
                            print_collision_rename_notice(
                                dest_dir,
                                output_filename,
                                [(new_filename, chosen_name)],
                                args.dry_run,
                            )

                    try:
                        success, _ = safe_file_operation(
                            "move",
                            orig_jpg_path,
                            dest_path,
                            "renaming",
                            args.force,
                            args.dry_run,
                        )
                    except KeyboardInterrupt:
                        interrupted = True
                        # Queued input moves are never started once we break
                        # out.  The remaining candidates are reported separately
                        # because only some of them would become operations.
                        interrupted_remaining = len(move_operations)
                        unprocessed_output_candidates = (
                            len(stack_output_order) - output_index
                        )
                        print(
                            "\nInterrupted by user. No new operations will be "
                            "started; completed renames and moves were left in "
                            "place and Stackcopy can be safely run again."
                        )
                        break
                    if success:
                        # Note: we do not mutate jpg_record["basename"] in-place here (per global rule)
                        processed_count += 1
                        if args.verbose or args.dry_run:
                            print(
                                format_action_message(
                                    operation_mode,
                                    output_filename,
                                    os.path.basename(dest_path),
                                    dest_dir,
                                    True,
                                    args.dry_run,
                                    bool(args.prefix),
                                )
                            )
                        output_move_success = True
                    else:
                        failed_count += 1
                        print(
                            f"Error: Failed to rename output file '{output_filename}'"
                        )
                else:
                    output_move_success = True

                if output_move_success:
                    processed_stems_for_remaining.add(output_stem)

                if output_move_success:
                    for input_stem in potential_inputs:
                        raw_record = file_db[input_stem]["files"].get("raw")
                        date_record = (
                            raw_record
                            or file_db[input_stem]["files"].get("ori")
                            or file_db[input_stem]["files"].get("jpg")
                        )
                        if not date_record:
                            continue

                        file_date = get_file_date(date_record, args.verbose)
                        if not file_date:
                            if args.verbose:
                                print(
                                    f"Warning: Could not determine date for '{input_stem}', skipping move."
                                )
                            continue

                        lightroom_dest_dir = os.path.join(
                            STACK_INPUT_DIR,
                            str(file_date.year),
                            file_date.strftime("%Y-%m-%d"),
                        )
                        stem_files = {
                            "jpg": file_db[input_stem]["files"].get("jpg"),
                            "raw": file_db[input_stem]["files"].get("raw"),
                            "ori": file_db[input_stem]["files"].get("ori"),
                        }
                        stem_files = {k: v for k, v in stem_files.items() if v}

                        try:
                            ensure_directory_once(
                                lightroom_dest_dir, created_dirs, args.dry_run
                            )
                        except OSError as directory_error:
                            failed_count += len(stem_files)
                            print(
                                f"Error creating destination directory "
                                f"'{lightroom_dest_dir}' for input stem "
                                f"'{input_stem}': {directory_error}"
                            )
                            print(
                                f"  {len(stem_files)} file(s) for '{input_stem}' "
                                "were left on the source."
                            )
                            continue

                        try:
                            counter, chosen = pick_unique_basenames_for_stem(
                                lightroom_dest_dir, stem_files, args.force, args.dry_run
                            )
                        except DestinationNameExhausted as exhausted:
                            name_exhausted_count += len(stem_files)
                            failed_count += len(stem_files)
                            print(
                                f"Error: cannot move input stem '{input_stem}': "
                                f"{exhausted}. Its files are left on the source; "
                                "nothing was overwritten."
                            )
                            continue
                        if counter > 1:
                            changes = []
                            for ft, fi in stem_files.items():
                                old = fi["basename"]
                                new = chosen.get(ft, old)
                                if new != old:
                                    changes.append((old, new))
                            key = (lightroom_dest_dir, input_stem)
                            if key not in collision_notified:
                                collision_notified.add(key)
                                print_collision_rename_notice(
                                    lightroom_dest_dir,
                                    input_stem,
                                    changes,
                                    args.dry_run,
                                )

                        for file_type in ["jpg", "raw", "ori"]:
                            file_info = file_db[input_stem]["files"].get(file_type)
                            if file_info:
                                src_path = file_info["path"]
                                if not os.path.exists(src_path):
                                    if args.verbose:
                                        print(
                                            f"Warning: File '{src_path}' missing at queue time, skipping."
                                        )
                                    continue

                                chosen_input_basename = chosen.get(
                                    file_type, file_info["basename"]
                                )
                                dest_path = os.path.join(
                                    lightroom_dest_dir, chosen_input_basename
                                )
                                move_operations.append(
                                    (
                                        src_path,
                                        dest_path,
                                        "moving input file",
                                        file_info["basename"],
                                        lightroom_dest_dir,
                                        input_stem,
                                    )
                                )
                                expected_moves_per_stem[input_stem] += 1

        # Execute collected moves for lightroom-only mode.  Skipped entirely if
        # Ctrl-C already stopped the detection/rename pass: no new operations
        # are started once the user has interrupted.
        if move_operations and not interrupted:
            ops_for_check = [(op[0], op[1], "move") for op in move_operations]
            confirm_if_low_space(ops_for_check, args.dry_run)

            if args.jobs > 1 and not args.dry_run:
                with ThreadPoolExecutor(max_workers=args.jobs) as executor:
                    future_to_op = {
                        executor.submit(
                            safe_file_operation,
                            "move",
                            src,
                            dst,
                            desc,
                            args.force,
                            args.dry_run,
                        ): (orig_name, ldest, inp_stem)
                        for src, dst, desc, orig_name, ldest, inp_stem in move_operations
                    }
                    accounted_futures: set = set()

                    def record_completed_move(future) -> None:
                        """Fold one finished future into the run counters."""
                        nonlocal total_bytes_moved, moved_input_count
                        nonlocal failed_count, lightroom_moves_attempted
                        nonlocal lightroom_source_remains_count
                        orig_name, ldest, inp_stem = future_to_op[future]
                        accounted_futures.add(future)
                        lightroom_moves_attempted += 1
                        try:
                            success, bytes_moved = future.result()
                        except Exception as e:
                            print(f"Error moving file '{orig_name}': {e}")
                            failed_count += 1
                            return
                        if success:
                            total_bytes_moved += bytes_moved
                            # The file is at its destination either way, so the
                            # stem still counts as fully placed; but a source
                            # still on the card was not moved, and must not be
                            # summarized as one.
                            if (
                                operation_outcome_of(success)
                                is OperationOutcome.COPIED_SOURCE_REMAINS
                            ):
                                lightroom_source_remains_count += 1
                            else:
                                moved_input_count += 1
                            input_dest_dirs.add(ldest)
                            successful_moves_per_stem[inp_stem] += 1
                            if args.verbose:
                                print(
                                    f"Moved input file '{orig_name}' to '{display_path(ldest)}'"
                                )
                        else:
                            failed_count += 1

                    try:
                        for future in as_completed(future_to_op):
                            record_completed_move(future)
                    except KeyboardInterrupt:
                        interrupted = True
                        # Drop queued work; already-running moves are atomic and
                        # are allowed to finish.
                        executor.shutdown(wait=True, cancel_futures=True)
                        # Ctrl-C can land between a worker finishing its move and
                        # the main thread consuming that future, and shutdown()
                        # lets in-flight moves finish.  Fold in every operation
                        # that actually completed so the partial summary counts
                        # it as done instead of "not started".
                        for pending_future in future_to_op:
                            if (
                                pending_future in accounted_futures
                                or pending_future.cancelled()
                                or not pending_future.done()
                            ):
                                continue
                            try:
                                record_completed_move(pending_future)
                            except KeyboardInterrupt:
                                # A worker that was itself interrupted never
                                # completed its move; it stays counted as
                                # started-but-unfinished.
                                pass
            else:
                try:
                    for (
                        src_path,
                        dest_path,
                        desc,
                        orig_name,
                        ldest,
                        inp_stem,
                    ) in move_operations:
                        lightroom_moves_attempted += 1
                        success, _ = safe_file_operation(
                            "move", src_path, dest_path, desc, args.force, args.dry_run
                        )
                        if success:
                            if (
                                operation_outcome_of(success)
                                is OperationOutcome.COPIED_SOURCE_REMAINS
                            ):
                                lightroom_source_remains_count += 1
                            else:
                                moved_input_count += 1
                            input_dest_dirs.add(ldest)
                            successful_moves_per_stem[inp_stem] += 1
                            if args.verbose or args.dry_run:
                                print(
                                    f"{'Would move' if args.dry_run else 'Moved'} input file '{orig_name}' to '{display_path(ldest)}'"
                                )
                        else:
                            failed_count += 1
                except KeyboardInterrupt:
                    interrupted = True

            if interrupted:
                interrupted_remaining = max(
                    0, len(move_operations) - lightroom_moves_attempted
                )
                print(
                    "\nInterrupted by user. No new operations will be started; "
                    "completed moves were left in place and Stackcopy can be "
                    "safely run again."
                )

        for stem, expected_count in expected_moves_per_stem.items():
            if successful_moves_per_stem[stem] == expected_count:
                processed_stems_for_remaining.add(stem)
            elif args.verbose:
                print(
                    f"Warning: Stem '{stem}' had partial move failure, leaving for 'remaining files' logic."
                )

    else:
        # --- Pre-flight disk check for Copy/Stackcopy ---
        # Skip for 'rename' mode as it is in-place (same filesystem).
        if operation_mode != "rename":
            ops_check_list = []

            for stem, data in file_db.items():
                if stem in ambiguous_stems:
                    continue
                if data.get("has_jpg") and not has_raw_like_companion(data):
                    jpg_record = data["files"].get("jpg")
                    if not jpg_record:
                        continue

                    filename = jpg_record["basename"]
                    if is_already_processed(filename):
                        continue

                    if target_date:
                        file_date = get_file_date(jpg_record, args.verbose)
                        if file_date is None or file_date != target_date:
                            continue

                    # Calculate actual destination path
                    dest_filename = filename
                    if operation_mode == "stackcopy" or args.prefix:
                        name_stem, ext = os.path.splitext(filename)
                        dest_filename = create_new_filename(name_stem, ext, args.prefix)

                    dest_path = os.path.join(dest_dir, dest_filename)

                    ops_check_list.append((jpg_record["path"], dest_path, "copy"))

            confirm_if_low_space(ops_check_list, args.dry_run)
        # --- End pre-flight check ---

        use_parallel_copy = (
            operation_mode in {"copy", "stackcopy"}
            and args.jobs > 1
            and not args.dry_run
        )

        if use_parallel_copy:
            with ThreadPoolExecutor(max_workers=args.jobs) as copy_executor:
                pending_copy_jobs = []
                for stem, data in file_db.items():
                    if stem in ambiguous_stems:
                        continue
                    # ... (logic to submit jobs)
                    if data.get("has_jpg") and not has_raw_like_companion(data):
                        # (The inner logic for submitting jobs remains the same)
                        jpg_record = data["files"].get("jpg")
                        if not jpg_record:
                            continue
                        jpg_path = jpg_record["path"]
                        filename = jpg_record["basename"]
                        name_stem, ext = os.path.splitext(filename)

                        if is_already_processed(filename):
                            if args.verbose:
                                print(
                                    f"Skipping '{filename}' because it already contains 'stacked'."
                                )
                            skipped_count += 1
                            continue

                        if target_date:
                            file_date = get_file_date(jpg_record, args.verbose)
                            if file_date is None or file_date != target_date:
                                continue

                        used_prefix = False

                        if operation_mode == "stackcopy":
                            new_filename = create_new_filename(
                                name_stem, ext, args.prefix
                            )
                            dest_path = os.path.join(dest_dir, new_filename)
                            used_prefix = True
                            future = copy_executor.submit(
                                safe_file_operation,
                                "copy",
                                jpg_path,
                                dest_path,
                                "copying",
                                args.force,
                                args.dry_run,
                            )
                            pending_copy_jobs.append(
                                {
                                    "future": future,
                                    "filename": filename,
                                    "dest_filename": new_filename,
                                    "dest_dir": dest_dir,
                                    "used_prefix": used_prefix,
                                }
                            )
                        elif operation_mode == "copy":
                            if args.prefix:
                                new_filename = create_new_filename(
                                    name_stem, ext, args.prefix
                                )
                                dest_path = os.path.join(dest_dir, new_filename)
                                used_prefix = True
                            else:
                                new_filename = filename
                                dest_path = os.path.join(dest_dir, filename)
                            future = copy_executor.submit(
                                safe_file_operation,
                                "copy",
                                jpg_path,
                                dest_path,
                                "copying",
                                args.force,
                                args.dry_run,
                            )
                            pending_copy_jobs.append(
                                {
                                    "future": future,
                                    "filename": filename,
                                    "dest_filename": new_filename,
                                    "dest_dir": dest_dir,
                                    "used_prefix": used_prefix,
                                }
                            )

                for job in pending_copy_jobs:
                    success = False
                    try:
                        success, bytes_copied = job["future"].result()
                        if success:
                            total_bytes_moved += bytes_copied
                            processed_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        print(f"Error processing '{job['filename']}': {e}")
                        failed_count += 1

                    if args.verbose or args.dry_run:
                        message = format_action_message(
                            operation_mode,
                            job["filename"],
                            job["dest_filename"],
                            job["dest_dir"],
                            success,
                            args.dry_run,
                            job["used_prefix"],
                        )
                        print(message)

        else:
            # Sequential processing logic (no ThreadPoolExecutor)
            for stem, data in file_db.items():
                if stem in ambiguous_stems:
                    continue
                if data.get("has_jpg") and not has_raw_like_companion(data):
                    jpg_record = data["files"].get("jpg")
                    if not jpg_record:
                        continue
                    jpg_path = jpg_record["path"]
                    filename = jpg_record["basename"]
                    name_stem, ext = os.path.splitext(filename)

                    if is_already_processed(filename):
                        if args.verbose:
                            print(
                                f"Skipping '{filename}' because it already contains 'stacked'."
                            )
                        skipped_count += 1
                        continue

                    if target_date:
                        file_date = get_file_date(jpg_record, args.verbose)
                        if file_date is None or file_date != target_date:
                            continue

                    used_prefix = False
                    success = None
                    dest_path = ""
                    new_filename = ""

                    if operation_mode == "rename":
                        new_filename = create_new_filename(name_stem, ext, args.prefix)
                        dest_path = os.path.join(dest_dir, new_filename)
                        used_prefix = bool(args.prefix)
                        success, _ = safe_file_operation(
                            "move",
                            jpg_path,
                            dest_path,
                            "renaming",
                            args.force,
                            args.dry_run,
                        )
                    elif operation_mode == "stackcopy":
                        new_filename = create_new_filename(name_stem, ext, args.prefix)
                        dest_path = os.path.join(dest_dir, new_filename)
                        used_prefix = True
                        success, _ = safe_file_operation(
                            "copy",
                            jpg_path,
                            dest_path,
                            "copying",
                            args.force,
                            args.dry_run,
                        )
                    elif operation_mode == "copy":
                        if args.prefix:
                            new_filename = create_new_filename(
                                name_stem, ext, args.prefix
                            )
                            dest_path = os.path.join(dest_dir, new_filename)
                            used_prefix = True
                        else:
                            new_filename = filename
                            dest_path = os.path.join(dest_dir, filename)
                        success, _ = safe_file_operation(
                            "copy",
                            jpg_path,
                            dest_path,
                            "copying",
                            args.force,
                            args.dry_run,
                        )

                    if success is not None:
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1

                        if args.verbose or args.dry_run:
                            message = format_action_message(
                                operation_mode,
                                filename,
                                os.path.basename(dest_path),
                                dest_dir,
                                success,
                                args.dry_run,
                                used_prefix,
                            )
                            print(message)

    # Print summary
    date_info = f" from {target_date}" if target_date else ""
    prefix_info = f" with prefix '{args.prefix}'" if args.prefix else ""

    if operation_mode == "lightroomimport":
        # Four mutually exclusive outcomes.  Every recognized file that was
        # acted on lands in exactly one of them:
        #
        #   normal    - placed where planned, and the source was removed
        #   recovered - placed at a fallback destination, source removed
        #   remains   - placed, but its source could not be removed
        #   failed    - never placed anywhere
        #
        # The per-category move counts already exclude source-remains files;
        # recovered_count does not, so that overlap is subtracted here.
        normal_placement_count = (
            moved_output_count + moved_input_count + remaining_moved_count
        )
        recovered_placement_count = recovered_count - recovered_source_remains_count
        source_remains_count = len(source_remains_paths)
        placed_count = (
            normal_placement_count + recovered_placement_count + source_remains_count
        )
        # "Imported" means the photo reached a destination *and* left the card.
        # A file whose source is still on the card is deliberately excluded and
        # reported on its own line below.
        imported_count = normal_placement_count + recovered_placement_count
        if interrupted:
            print(
                f"Import interrupted. Completed: {imported_count}; failed: {failed_count}; "
                f"remaining: {interrupted_remaining}."
            )
        elif args.dry_run:
            print(
                f"DRY RUN complete. {imported_count} files would be {past_tense_verb.lower()}."
            )
        else:
            throughput_info = ""
            if total_bytes_moved > 0:
                total_gb = total_bytes_moved / (1000**3)
                mbps = 0
                if exec_elapsed_time > 0:
                    mbps = (total_bytes_moved / (1000**2)) / exec_elapsed_time
                throughput_info = (
                    f"Data: {total_gb:.1f} GB at {mbps:.1f} MB/s average. "
                )

            import_action = "Copied" if args.leave_on_card else "Imported"
            source_note = "Sources left in place. " if args.leave_on_card else ""
            recovery_breakdown = (
                f", {recovered_placement_count} recovered to the Lightroom hierarchy"
                if recovered_placement_count
                else ""
            )
            # The breakdown sums to imported_count exactly: stacked outputs,
            # stack inputs and remaining files are the normal placements, plus
            # the recoveries whose sources were removed.
            print(
                f"Done. {import_action} {imported_count} files in {exec_elapsed_time:.1f}s. "
                f"{source_note}"
                f"Breakdown: {len(moved_stack_groups)} {'stack' if len(moved_stack_groups) == 1 else 'stacks'} "
                f"({moved_output_count} stacked outputs, {moved_input_count} input files), "
                f"{remaining_moved_count} remaining{recovery_breakdown}. "
                f"{throughput_info}Failures: {failed_count}."
            )
            if source_remains_count or recovered_count or failed_count:
                print(
                    f"\nFiles safely placed: {placed_count}"
                    f"\n  Imported normally: {normal_placement_count}"
                    f"\n  Recovered to fallback destination: "
                    f"{recovered_placement_count}"
                    f"\n  Copied successfully but source could not be removed: "
                    f"{source_remains_count}"
                    f"\nFailures: {failed_count}"
                )
            if primary_failure_count:
                print(
                    f"Recovery: {primary_failure_count} primary placement failures; "
                    f"{recovered_count} recovered; {failed_count} unrecovered."
                )
            if recovered_count:
                print(
                    f"\nImport completed with recovery.\n"
                    f"{recovered_count} file(s) could not be placed as planned "
                    "and were recovered to:"
                )
                for recovery_dir in sorted(recovery_dest_dirs):
                    print(f"  {recovery_dir}")
                print("No data was lost, but manual review is recommended.")

    elif operation_mode == "lightroom" and interrupted:
        # Partial summary: never claim the run completed.
        print(
            f"\nLightroom processing interrupted. Renamed {processed_count} stacked "
            f"output(s) and moved {moved_input_count} input file(s); "
            f"failed: {failed_count}; queued moves not started: "
            f"{interrupted_remaining}."
        )
        if lightroom_source_remains_count:
            print(
                f"{lightroom_source_remains_count} input file(s) reached the "
                "destination but their sources could not be removed; they are "
                "listed below and are not counted as moved."
            )
        if unprocessed_output_candidates:
            print(
                f"{unprocessed_output_candidates} stacked-output candidate(s) were "
                "left unprocessed."
            )
        print(
            "Completed operations were left in place and unstarted files were "
            "left untouched. Re-running Stackcopy is safe."
        )
        if input_dest_dirs:
            print("Input files moved to:")
            for d in sorted(input_dest_dirs):
                print(f"  - {d}")
    elif args.dry_run:
        # Custom summary for dry-run
        if operation_mode == "rename":
            print(
                f"\nDRY RUN: Would rename {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'."
            )
        elif operation_mode == "lightroom":
            print(
                f"\nDRY RUN: Would process {stack_outputs_seen} stacked JPG files"
                f"{prefix_info} in '{src_dir}' (renaming {processed_count} of them)."
            )
            print(
                f"Input sequences not all RAW-backed skipped: {inputs_not_all_raw_backed_skipped}"
            )
            print(
                f"DRY RUN: Would move {moved_input_count} input files (JPG, RAW, and ORI) to:"
            )
            for d in sorted(input_dest_dirs):
                print(f"  - {d}")
        elif operation_mode == "stackcopy":
            print(
                f"\nDRY RUN: Would copy and rename {processed_count} JPG files{prefix_info} without corresponding raw files to the '{dest_dir}' directory."
            )
        else:  # copy mode
            action_desc = "copy and rename" if args.prefix else "copy"
            print(
                f"\nDRY RUN: Would {action_desc} {processed_count} JPG files{prefix_info}{date_info} without corresponding raw files to '{dest_dir}'."
            )
    else:
        # Normal summary
        if operation_mode == "rename":
            print(
                f"\nDone. Renamed {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'."
            )
        elif operation_mode == "lightroom":
            print(
                f"\nDone. Processed {stack_outputs_seen} stacked JPG files"
                f"{prefix_info} in '{src_dir}' (renamed {processed_count})."
            )
            print(
                f"Input sequences not all RAW-backed skipped: {inputs_not_all_raw_backed_skipped}"
            )
            print(f"Moved {moved_input_count} input files (JPG, RAW, and ORI) to:")
            for d in sorted(input_dest_dirs):
                print(f"  - {d}")
            if lightroom_source_remains_count:
                print(
                    f"{lightroom_source_remains_count} input file(s) reached the "
                    "destination but their sources could not be removed; they are "
                    "listed below and are not counted as moved."
                )
        elif operation_mode == "stackcopy":
            print(
                f"\nDone. Copied and renamed {processed_count} JPG files{prefix_info} without corresponding raw files to the '{dest_dir}' directory."
            )
        else:  # copy mode
            action_desc = "Copied and renamed" if args.prefix else "Copied"
            print(
                f"\nDone. {action_desc} {processed_count} JPG files{prefix_info}{date_info} without corresponding raw files to '{dest_dir}'."
            )

    if source_remains_paths:
        print(
            f"\nSources still in place after a successful copy: "
            f"{len(source_remains_paths)}"
        )
        print(
            "These files are safely at their destination, but their originals "
            "were not removed:"
        )
        for remaining_source in source_remains_paths:
            print(f"  {remaining_source}")
        print(
            "Re-running is safe: the matching destination files are recognized "
            "and not duplicated."
        )

    if forced_overwrite_paths:
        label = (
            "Existing files that would be overwritten by --force"
            if args.dry_run
            else "Existing files overwritten by --force"
        )
        print(f"\n{label}: {len(forced_overwrite_paths)}")

    if skipped_count > 0:
        print(f"Skipped {skipped_count} files that were already processed.")

    if ambiguous_file_count:
        print(
            f"\nAmbiguous stems left untouched: {len(ambiguous_stems)} "
            f"({ambiguous_file_count} files). Rename or separate the files "
            "listed above, then run Stackcopy again."
        )

    if unrecognized_extensions:
        unrecognized_count = sum(unrecognized_extensions.values())
        print(f"Unrecognized files left on source: {unrecognized_count}")
        for extension, count in sorted(unrecognized_extensions.items()):
            print(f"  {extension}: {count}")

    if failed_count > 0:
        print(f"Failed to process {failed_count} files.")

    if name_exhausted_count > 0:
        outcome = "would remain on" if args.dry_run else "were left on"
        print(
            f"No collision-free destination name could be found for "
            f"{name_exhausted_count} file(s); they {outcome} the source. "
            "Free up the conflicting destination names and run again."
        )

    if interrupted:
        sys.exit(130)

    # Return non-zero status if any execution failures occurred (excluding
    # dry-run), and also for outcomes that are not failures but are not an
    # ordinary success either.  Exhausted destination names and ambiguous
    # stems always fail the run, dry runs included: the import cannot be
    # completed as planned and must not report success.
    #
    # "Degraded" means the files are safe but the run did not do what it set
    # out to do - something was recovered to a fallback destination, or a copy
    # succeeded while its source stayed on the card.  Reporting either as a
    # plain exit 0 would tell the user their card is ready to format.
    degraded = bool(recovered_count) or bool(source_remains_paths)
    if (
        name_exhausted_count > 0
        or ambiguous_file_count > 0
        or degraded
        or (not args.dry_run and failed_count > 0)
    ):
        if args.lightroomimport is not None and partial_failures_found:
            print(
                "\nWARNING: Some stems partially failed. Check logs above for details."
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
