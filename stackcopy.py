#!/usr/bin/python3
# SPDX-License-Identifier: MIT

# Stackcopy version 1.5.8 by Alan Rockefeller
# 7/14/26

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
from bisect import bisect_left
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any

STACKCOPY_VERSION = "1.5.8"

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
IS_WSL = _is_wsl()

_wsl_warning_shown = False


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

NUMERIC_STEM_REGEX = re.compile(r"([a-zA-Z0-9_-]*)(\d{6,})")
CAMERA_ROLL_DIR_REGEX = re.compile(r"^(\d{3})([A-Za-z0-9_]{5})$")

# Stack-detection timing/burst thresholds (shared by the --lightroomimport and
# --lightroom backward-scan passes).
MAX_OUTPUT_LAG_SECONDS = 120
MAX_INPUT_GAP_SECONDS = 6
MAX_BURST_GAP_SECONDS = 2.0
MIN_STACK_INPUT_FRAMES = 3
MAX_STACK_INPUT_FRAMES = 15
BURST_EXTRA_FRAMES_REQUIRED = 3


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
    file_type: str  # "jpg", "raw", or "video"
    mtime: datetime | None
    basename_orig: str  # original filename
    basename_dest: (
        str  # destination filename (may include "stacked" rename + collision suffix)
    )
    dest_dir: str


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
        except OSError as e:
            if verbose:
                print(f"Warning: Could not stat file '{file_record['path']}': {e}")
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


def _path_is_within(path: str, root: str) -> bool:
    """True if `path` equals `root` or is nested within it (both absolute)."""
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        # Different drives (Windows) or an abs/relative mismatch -> not within.
        return False


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
                _path_is_within(os.path.realpath(entry.path), excl)
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

    jpg_files = record["files"].get("jpg")
    if jpg_files:
        mtime = get_file_mtime(jpg_files, verbose)
        if mtime:
            return mtime

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


def add_counter_suffix(basename: str, counter: int) -> str:
    """
    Insert a counter suffix before the extension: IMG.JPG -> IMG__2.JPG
    Counter=1 returns the original basename.
    """
    if counter <= 1:
        return basename
    stem, ext = os.path.splitext(basename)
    return f"{stem}__{counter}{ext}"


def dest_conflicts(src_path: str, dest_path: str, force: bool) -> bool:
    """
    Return True if dest_path exists and we should NOT overwrite it.
    If contents are identical, it's not a conflict (we treat it as safe).
    If --force is set, we consider it not a conflict (user explicitly wants overwrite).
    """
    if not os.path.exists(dest_path):
        return False
    if force:
        return False
    # If source exists and is identical to destination, it's safe to treat as non-conflict.
    return not (os.path.exists(src_path) and files_identical(src_path, dest_path))


def pick_unique_basenames_for_stem(
    dest_dir: str,
    files_by_type: dict[str, Any],
    force: bool,
    _dry_run: bool,
    reserved_paths: set[str] | None = None,
) -> tuple[int, dict[str, str]]:
    """
    Choose a single counter for *all* files in this stem (e.g., JPG+ORF) so they stay paired.
    Returns (counter, {file_type: chosen_basename}).
    Only applies counter suffixing when a destination collision exists and --force is NOT set.

    reserved_paths: optional set of destination paths already claimed by earlier planned
    moves (used by --lightroomimport plan-then-execute to avoid two planned moves
    targeting the same path).
    """
    orig = {ft: fi["basename"] for ft, fi in files_by_type.items() if fi}
    srcs = {ft: fi["path"] for ft, fi in files_by_type.items() if fi}

    # If force is on, do not auto-rename; user asked to overwrite.
    if force:
        return 1, orig

    # Try counters starting at 1 until all destinations are non-conflicting.
    # (Hard cap avoids infinite loops in weird cases.)
    for counter in range(1, 1000):
        chosen = {}
        ok = True
        for ft, basename in orig.items():
            candidate_basename = add_counter_suffix(basename, counter)
            candidate_path = os.path.join(dest_dir, candidate_basename)
            if dest_conflicts(srcs[ft], candidate_path, force=False):
                ok = False
                break
            if reserved_paths and candidate_path in reserved_paths:
                ok = False
                break
            chosen[ft] = candidate_basename
        if ok:
            return counter, chosen

    # Fallback (extremely unlikely): return the last tried mapping.
    return 999, {ft: add_counter_suffix(bn, 999) for ft, bn in orig.items()}


def print_collision_rename_notice(
    dest_dir: str, stem_label: str, changes: list[tuple[str, str]], dry_run: bool
) -> None:
    """
    Always prints (even without --verbose) when we rename due to destination collisions.
    """
    if not changes:
        return
    verb = "Would rename" if dry_run else "Renaming"
    why = (
        "a file with the same name already exists in the destination "
        "(usually from an earlier import after the camera card counter reset)"
    )
    print(
        f"Note: {verb} files for '{stem_label}' in '{dest_dir}' to avoid overwriting earlier photos ({why})"
    )
    for old, new in changes:
        print(f"  - {old} -> {new}")


def _atomic_copy2(src_path: str, dest_path: str) -> None:
    """Copy to a temp file in dest dir, then atomically replace dest_path."""
    dest_dir = os.path.dirname(dest_path)
    os.makedirs(dest_dir, exist_ok=True)
    tmp_path = os.path.join(
        dest_dir, f".__stackcopy_tmp__{os.path.basename(dest_path)}.{uuid.uuid4().hex}"
    )
    try:
        shutil.copy2(src_path, tmp_path)
        # Atomic replace: dest_path is either old or new, never partial
        os.replace(tmp_path, dest_path)
    finally:
        # Clean up temp if something went wrong before replace
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


def safe_file_operation(
    operation, src_path, dest_path, operation_name, force=False, dry_run=False
):
    """
    Perform a safe file copy or move.
    Returns: (success_bool, bytes_copied_count)
    """
    # Determine size before operation in case of move/deletion
    src_size = 0
    if os.path.exists(src_path):
        try:
            src_size = os.path.getsize(src_path)
        except OSError:
            pass

    # If the source and destination are the same file, there is nothing to do.
    # Without this guard, the "identical content" branch below would unlink the
    # source -- which is also the destination -- and destroy the only copy.
    try:
        if (
            os.path.exists(src_path)
            and os.path.exists(dest_path)
            and os.path.samefile(src_path, dest_path)
        ):
            return True, 0
    except OSError:
        pass

    # If destination exists and we're not forcing, see if it's identical to the source.
    if os.path.exists(dest_path) and not force:
        # Check self-heal first (applies to both dry-run and real)
        is_self_heal = False
        try:
            if os.path.exists(src_path):
                src_size_curr = os.path.getsize(src_path)
                dst_size = os.path.getsize(dest_path)
                # Self-heal: if destination is a 0-byte placeholder but source is non-zero
                if src_size_curr > 0 and dst_size == 0:
                    is_self_heal = True
        except OSError:
            pass

        if is_self_heal:
            msg = (
                "replacing from source" if not dry_run else "would replace from source"
            )
            print(f"Note: destination '{dest_path}' exists but is 0 bytes; {msg}.")
            force = True
        elif dry_run:
            print(
                f"Warning: '{dest_path}' already exists. Would need --force to overwrite."
            )
            return False, 0
        elif os.path.exists(src_path):
            # If contents are identical, treat this as success.
            if files_identical(src_path, dest_path):
                if operation == "move":
                    try:
                        os.unlink(src_path)
                    except OSError as delete_error:
                        print(
                            f"Note: destination '{dest_path}' already exists with identical content; "
                            f"source '{src_path}' could not be deleted: {delete_error}"
                        )
                    else:
                        print(
                            f"Note: destination '{dest_path}' already exists with identical content; "
                            f"deleted source '{src_path}'."
                        )
                    return True, 0
                else:
                    print(
                        f"Note: destination '{dest_path}' already exists with identical content; "
                        f"skipping copy from '{src_path}'."
                    )
                    return True, 0

            print(f"Warning: '{dest_path}' already exists. Use --force to overwrite.")
            return False, 0
        else:
            print(f"Warning: '{dest_path}' already exists. Use --force to overwrite.")
            return False, 0

    if dry_run:
        return True, 0

    try:
        if operation == "move":
            try:
                # Same-filesystem fast path (metadata only)
                os.replace(src_path, dest_path)
                return True, 0
            except OSError as move_error:
                if move_error.errno == errno.EXDEV:
                    # Cross-device: atomic copy then delete source
                    _atomic_copy2(src_path, dest_path)
                    try:
                        os.unlink(src_path)
                    except OSError as delete_error:
                        print(
                            f"Note: {operation_name} '{src_path}' to '{dest_path}' completed, "
                            f"but source could not be deleted: {delete_error}"
                        )
                    # For cross-device moves, bytes were physically moved
                    return True, src_size
                else:
                    raise
        elif operation == "copy":
            _atomic_copy2(src_path, dest_path)
            return True, src_size

        return True, 0

    except (OSError, shutil.Error) as e:
        print(f"Error {operation_name} '{src_path}' to '{dest_path}': {e}")
        return False, 0


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

            if os.environ.get("STACKCOPY_ASSUME_YES") == "1":
                if not _LOW_SPACE_REPORTS_ENABLED:
                    print(
                        "Proceeding despite low space (confirmed via STACKCOPY_ASSUME_YES)."
                    )
                _confirmed_filesystems.add(dev_id)
                continue

            if _LOW_SPACE_REPORTS_ENABLED:
                _emit_low_space_report(report)
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
        help="Move input files (JPG and ORF) to a dated directory structure and rename the stacked JPG in place.",
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
    date_group = parser.add_argument_group(
        "Date Filtering (optional, for copy operations)"
    )
    date_group.add_argument(
        "--today",
        action="store_true",
        help="Process JPGs created today that don't have a corresponding raw file.",
    )
    date_group.add_argument(
        "--yesterday",
        action="store_true",
        help="Process JPGs created yesterday that don't have a corresponding raw file.",
    )
    date_group.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Process JPGs from a specific date that don't have a corresponding raw file.",
    )

    # Add prefix option
    parser.add_argument(
        "--prefix",
        metavar="PREFIX",
        help="Add a custom prefix before ' stacked' in the filename when using --copy, --rename, or --stackcopy.",
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
        "-j",
        "--jobs",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel copy workers to use for --copy/--stackcopy (default: 1)",
    )

    # Parse arguments
    # --- 0. Execution Tracking & Summary Statistics ---
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    moved_input_count = 0
    moved_output_count = 0
    stack_outputs_seen = 0
    remaining_moved_count = 0
    inputs_not_all_raw_backed_skipped = 0
    total_bytes_moved = 0
    exec_start_time = None
    exec_elapsed_time = 0
    partial_failures_found = False
    execution_results: dict[str, dict] = {}

    args = parser.parse_args()

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
        # If user didn't explicitly request more jobs, pick something sensible
        # 4 workers max, but don't exceed 2x CPU cores
        auto_jobs = min(4, cpu_count * 2)
        if args.verbose:
            print(f"Auto-selecting {auto_jobs} worker jobs for Lightroom mode.")
        args.jobs = auto_jobs

    # lightroomimport always runs sequentially so files move in oldest-first order
    if args.lightroomimport is not None:
        args.jobs = 1

    created_dirs = set()

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

        # Ensure the stack input directory exists
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
    REMAINING_FILE_TYPES = ("jpg", "raw", "video")

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
    #     "has_raw": bool,
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
    try:
        for entry in iter_source_file_entries(
            src_dir, recursive=scan_recursively, exclude_dirs=scan_exclude_dirs
        ):
            stem, ext = os.path.splitext(entry.name)
            ext_lower = ext.lower()
            relative_dir = os.path.relpath(os.path.dirname(entry.path), src_dir)
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
                numeric_match = NUMERIC_STEM_REGEX.match(stem)
                if numeric_match:
                    prefix, num_str = numeric_match.groups()
                    record["numeric"] = {
                        "prefix": prefix,
                        "num": int(num_str),
                        "width": len(num_str),
                    }

            if ext_lower in RAW_EXTENSIONS:
                record["has_raw"] = True
                record["files"]["raw"] = file_meta
            elif ext_lower in JPG_EXTENSIONS:
                record["has_jpg"] = True
                record["files"]["jpg"] = file_meta
            elif ext_lower in VIDEO_EXTENSIONS:
                record["has_video"] = True
                record["files"]["video"] = file_meta

    except OSError as e:
        print(f"Error scanning source directory '{src_dir}': {e}")
        sys.exit(1)

    known_relative_dirs_by_key = {
        _relative_dir_lookup_key(record.get("relative_dir", ".")): record.get(
            "relative_dir", "."
        )
        for record in file_db.values()
    }

    sequences_by_prefix = {}
    for stem, record in file_db.items():
        numeric_info = record.get("numeric")
        if numeric_info and (record.get("has_raw") or record.get("has_jpg")):
            sequence_key = (record.get("relative_dir", "."), numeric_info["prefix"])
            sequences_by_prefix.setdefault(sequence_key, []).append(
                (numeric_info["num"], stem)
            )
    for prefix in sequences_by_prefix:
        sequences_by_prefix[prefix].sort()

    def get_stack_sequence(output_record, prefix):
        relative_dir = output_record.get("relative_dir", ".")
        sequence_dirs = [relative_dir]
        if scan_recursively:
            previous_dir = previous_adjacent_camera_dir(
                relative_dir, known_relative_dirs_by_key
            )
            if previous_dir:
                sequence_dirs.insert(0, previous_dir)

        sequence = []
        sequence_dirs_with_matches = []
        for sequence_dir in sequence_dirs:
            dir_sequence = sequences_by_prefix.get((sequence_dir, prefix))
            if dir_sequence:
                sequence.extend(dir_sequence)
                sequence_dirs_with_matches.append(sequence_dir)

        if len(sequence_dirs_with_matches) == 1:
            return sequence, sequence_dirs_with_matches

        # sequence_dirs is ordered fallback-first, current-dir-last; assignment
        # therefore keeps the current folder's frame when counters overlap.
        stems_by_num = {}
        for sequence_dir in sequence_dirs:
            for num, stem in sequences_by_prefix.get((sequence_dir, prefix), ()):
                stems_by_num[num] = stem
        sequence = sorted(stems_by_num.items())
        return sequence, sequence_dirs_with_matches

    def stack_detection_group_key(record):
        """Group files for stack detection by folder and numeric filename prefix."""
        numeric_info = record.get("numeric")
        prefix = numeric_info["prefix"] if numeric_info else None
        return (record.get("relative_dir", "."), prefix)

    # Reliable stack detection needs RAW+JPG pairing; in groups (folder +
    # numeric filename prefix) that have JPGs but no RAW at all, disable it.
    # Only the Lightroom modes consume this, so skip the scan otherwise.
    jpg_only_stack_groups = set()
    if operation_mode in ("lightroomimport", "lightroom"):
        groups_with_jpg = set()
        groups_with_raw = set()
        for record in file_db.values():
            has_jpg = record.get("has_jpg")
            has_raw = record.get("has_raw")
            if not (has_jpg or has_raw):
                continue
            group = stack_detection_group_key(record)
            if has_jpg:
                groups_with_jpg.add(group)
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

    def warn_inputs_not_all_raw_backed(record):
        relative_dir = record.get("relative_dir", ".")
        if relative_dir in warned_inputs_not_all_raw_backed_dirs:
            return
        warned_inputs_not_all_raw_backed_dirs.add(relative_dir)
        print(
            "Stack detection skipped in "
            f"{describe_stack_detection_folder(record)}: inferred input frames "
            "are not all RAW-backed. Enable RAW+JPG for "
            "automatic stack sorting."
        )

    def inferred_inputs_are_raw_backed(input_stems):
        return bool(input_stems) and all(
            file_db[input_stem].get("has_raw") for input_stem in input_stems
        )

    def find_stack_output_candidates():
        """Return JPG-without-RAW stems, excluding JPG-only detection groups."""
        if args.no_stack_detection:
            if args.debug_stacks:
                print("Stack detection disabled by --no-stack-detection.")
            return set()

        stacked_outputs = set()
        for stem, data in file_db.items():
            if not (data.get("has_jpg") and not data.get("has_raw")):
                continue

            jpg_record = data["files"].get("jpg")
            if not jpg_record:
                continue

            if target_date:
                file_date = get_file_date(jpg_record, args.verbose)
                if file_date is None or file_date != target_date:
                    continue

            group = stack_detection_group_key(data)
            if group in jpg_only_stack_groups:
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

            if not (candidate_record.get("has_raw") or candidate_record.get("has_jpg")):
                stop_reason = "No corresponding RAW or JPG file found"
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                break

            if (
                not candidate_record.get("has_raw")
                and len(potential_inputs) >= MIN_STACK_INPUT_FRAMES
            ):
                stop_reason = "Non-RAW-backed boundary after sufficient inputs"
                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': STOPPED ({stop_reason})")
                break

            input_mtime = get_stem_mtime(candidate_record, args.verbose)

            has_valid_raw_mtime = False
            if candidate_record.get("has_raw"):
                raw_mtime_val = get_file_mtime(candidate_record["files"]["raw"], False)
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
            warn_inputs_not_all_raw_backed(output_data)
            if args.debug_stacks:
                print(
                    "  - Stack REJECTED: inferred input frames are not all RAW-backed; automatic stack detection requires RAW-backed input frames."
                )

        return potential_inputs, too_many_in_burst, inputs_not_all_raw_backed

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
        processed_stems_for_remaining = set()

        planned_moves: list[PlannedMove] = []
        accepted_stacks = 0
        skipped_claimed_as_input = 0
        rejected_no_numeric_stem = 0
        rejected_no_sequence = 0
        rejected_first_in_sequence = 0
        rejected_no_mtime = 0
        rejected_too_few_inputs = 0
        rejected_burst_safety = 0
        skipped_missing_date = 0
        skipped_missing_at_plan = 0

        for output_stem in sorted(stacked_outputs, reverse=True):
            if output_stem in claimed_input_stems:
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

            numeric_info = output_data.get("numeric")
            if not numeric_info:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output JPG has no numeric stem.")
                rejected_no_numeric_stem += 1
                continue

            prefix = numeric_info["prefix"]
            output_num = numeric_info["num"]
            sequence, sequence_dirs = get_stack_sequence(output_data, prefix)

            if args.debug_stacks:
                print(f"  - Numeric Stem Info: prefix='{prefix}', number={output_num}")
                if len(sequence_dirs) > 1:
                    print(
                        "  - Sequence includes adjacent camera folders: "
                        + ", ".join(sequence_dirs)
                    )

            if not sequence:
                if args.debug_stacks:
                    print("  - Stack REJECTED: No sequence found for this prefix.")
                rejected_no_sequence += 1
                continue

            idx = bisect_left(sequence, (output_num, ""))
            if idx == 0:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output is the first in its sequence.")
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

            # Final decision on the stack
            is_valid_stack = (
                (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                )
                and not too_many_in_burst
                and not inputs_not_all_raw_backed
            )

            if args.debug_stacks:
                print(
                    f"  - Final Decision: {'ACCEPTED' if is_valid_stack else 'REJECTED'}"
                )
                if not (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                ):
                    print(
                        f"    - Reason: Found {len(potential_inputs)} inputs, but requires "
                        f"{MIN_STACK_INPUT_FRAMES}-{MAX_STACK_INPUT_FRAMES}."
                    )
                if too_many_in_burst:
                    print(
                        "    - Reason: Burst safety check failed (likely a focus bracket)."
                    )
                if inputs_not_all_raw_backed:
                    print("    - Reason: Inferred input frames are not all RAW-backed.")
                print("--- End Debugging Stack ---")

            if not is_valid_stack:
                if not inputs_not_all_raw_backed and not (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                ):
                    rejected_too_few_inputs += 1
                if too_many_in_burst:
                    rejected_burst_safety += 1
                continue

            # --- Stack accepted: plan moves ---
            accepted_stacks += 1
            for input_stem in potential_inputs:
                claimed_input_stems.add(input_stem)

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
                counter, chosen = pick_unique_basenames_for_stem(
                    dest_dir_import,
                    out_files,
                    args.force,
                    args.dry_run,
                    reserved_paths=reserved_dest_paths,
                )
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
                reserved_dest_paths.add(dest_path)
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
                    )
                )
                processed_stems_for_remaining.add(output_stem)
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
                    if raw_record
                    else file_db[input_stem]["files"].get("jpg")
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
                }
                stem_files = {k: v for k, v in stem_files.items() if v}
                counter, chosen = pick_unique_basenames_for_stem(
                    lightroom_dest_dir,
                    stem_files,
                    args.force,
                    args.dry_run,
                    reserved_paths=reserved_dest_paths,
                )
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

                planned_any = False
                for file_type in ["jpg", "raw"]:
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
                    reserved_dest_paths.add(dest_path)
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
                        )
                    )
                    planned_any = True

                if planned_any:
                    processed_stems_for_remaining.add(input_stem)

        # A3. Plan remaining files
        for stem, record in file_db.items():
            files_by_dest: dict[str, list[tuple[str, dict]]] = defaultdict(list)
            for file_type in REMAINING_FILE_TYPES:
                if file_type != "video" and stem in processed_stems_for_remaining:
                    continue

                file_info = record["files"].get(file_type)
                if not file_info:
                    continue

                src_path = file_info["path"]
                if not os.path.exists(src_path):
                    skipped_missing_at_plan += 1
                    continue

                file_date = get_file_date(file_info, args.verbose)
                if file_date is None:
                    skipped_missing_date += 1
                    if args.verbose:
                        print(
                            f"Warning: Could not determine date for '{src_path}', skipping import move."
                        )
                    continue

                dest_dir_import = os.path.join(
                    lightroom_import_base_dir,
                    str(file_date.year),
                    file_date.strftime("%Y-%m-%d"),
                )
                files_by_dest[dest_dir_import].append((file_type, file_info))

            for dest_dir_import, files in files_by_dest.items():
                stem_files_for_dest = dict(files)
                counter, chosen = pick_unique_basenames_for_stem(
                    dest_dir_import,
                    stem_files_for_dest,
                    args.force,
                    args.dry_run,
                    reserved_paths=reserved_dest_paths,
                )
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
                    reserved_dest_paths.add(dest_path)
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

        # --- Phase B: Disk space preflight (unified) ---
        if planned_moves:
            ops_for_check = [
                (m.src_path, m.dest_path, file_operation) for m in planned_moves
            ]
            confirm_if_low_space(ops_for_check, args.dry_run)

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

        verb = planned_verb
        dry_prefix = "DRY RUN: " if args.dry_run else ""

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

        # --- Phase E: Interactive confirmation ---
        if args.interactive:
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
        for _move_index, move in enumerate(planned_moves):
            _emit_progress(
                file=os.path.basename(move.src_path),
                phase=file_operation,
                done=_move_index,
                total=_total_planned,
            )
            ensure_directory_once(move.dest_dir, created_dirs, args.dry_run)

            success, bytes_moved = safe_file_operation(
                file_operation,
                move.src_path,
                move.dest_path,
                f"{operation_label} {move.category.replace('_', ' ')} file",
                args.force,
                args.dry_run,
            )

            res = execution_results[move.stem]
            if not args.dry_run:
                if success:
                    res["succeeded"] += 1
                    if move.category != "remaining":
                        res["stack_succeeded"] += 1
                else:
                    res["failed"] += 1
            res["moves"].append({"move": move, "success": success})

            if success:
                total_bytes_moved += bytes_moved
                if move.category == "stack_output":
                    moved_output_count += 1
                    processed_count += 1
                elif move.category == "stack_input":
                    moved_input_count += 1
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
            else:
                failed_count += 1

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
                        status = "SUCCESS" if m_res["success"] else "FAILED"
                        print(f"  [{status}] {move.basename_orig} -> {move.dest_path}")
                    print("*********************************************\n")

        if recovery_stems:
            if args.verbose:
                print(
                    f"\nRecovering {len(recovery_stems)} stem(s) whose planned moves all failed..."
                )
            for stem in recovery_stems:
                record = file_db[stem]
                files_by_dest: dict[str, list[tuple[str, dict]]] = defaultdict(list)
                for file_type in REMAINING_FILE_TYPES:
                    file_info = record["files"].get(file_type)
                    if not file_info:
                        continue
                    src_path = file_info["path"]
                    if not os.path.exists(src_path):
                        continue
                    file_date = get_file_date(file_info, args.verbose)
                    if file_date is None:
                        continue
                    dest_dir_import = os.path.join(
                        lightroom_import_base_dir,
                        str(file_date.year),
                        file_date.strftime("%Y-%m-%d"),
                    )
                    files_by_dest[dest_dir_import].append((file_type, file_info))

                for dest_dir_import, files in files_by_dest.items():
                    ensure_directory_once(dest_dir_import, created_dirs, args.dry_run)
                    stem_files_for_dest = dict(files)
                    counter, chosen = pick_unique_basenames_for_stem(
                        dest_dir_import,
                        stem_files_for_dest,
                        args.force,
                        args.dry_run,
                    )
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

                    for ft, file_info in files:
                        file_dest_basename = chosen.get(ft, file_info["basename"])
                        dest_path = os.path.join(dest_dir_import, file_dest_basename)

                        success, bytes_moved = safe_file_operation(
                            file_operation,
                            file_info["path"],
                            dest_path,
                            f"{operation_label} recovered remaining file",
                            args.force,
                            args.dry_run,
                        )
                        if success:
                            total_bytes_moved += bytes_moved
                            remaining_moved_count += 1
                            if args.verbose or args.dry_run:
                                verb = done_verb
                                dest_short = display_path(dest_dir_import)
                                print(
                                    f"{verb} remaining '{file_info['basename']}' as '{file_dest_basename}' -> '{dest_short}'"
                                    if file_dest_basename != file_info["basename"]
                                    else f"{verb} remaining '{file_info['basename']}' -> '{dest_short}'"
                                )
                        else:
                            failed_count += 1

        if exec_start_time is not None:
            exec_elapsed_time = time.perf_counter() - exec_start_time

        _emit_progress(phase="done", done=len(planned_moves), total=len(planned_moves))

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

        for output_stem in sorted(stacked_outputs, reverse=True):
            if output_stem in claimed_input_stems:
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

            numeric_info = output_data.get("numeric")
            if not numeric_info:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output JPG has no numeric stem.")
                continue

            prefix = numeric_info["prefix"]
            output_num = numeric_info["num"]
            sequence, sequence_dirs = get_stack_sequence(output_data, prefix)

            if args.debug_stacks:
                print(f"  - Numeric Stem Info: prefix='{prefix}', number={output_num}")
                if len(sequence_dirs) > 1:
                    print(
                        "  - Sequence includes adjacent camera folders: "
                        + ", ".join(sequence_dirs)
                    )

            if not sequence:
                if args.debug_stacks:
                    print("  - Stack REJECTED: No sequence found for this prefix.")
                continue

            idx = bisect_left(sequence, (output_num, ""))
            if idx == 0:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output is the first in its sequence.")
                continue

            if output_mtime is None:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output mtime is missing.")
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
                (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                )
                and not too_many_in_burst
                and not inputs_not_all_raw_backed
            )

            if args.debug_stacks:
                print(
                    f"  - Final Decision: {'ACCEPTED' if is_valid_stack else 'REJECTED'}"
                )
                if not (
                    MIN_STACK_INPUT_FRAMES
                    <= len(potential_inputs)
                    <= MAX_STACK_INPUT_FRAMES
                ):
                    print(
                        f"    - Reason: Found {len(potential_inputs)} inputs, but requires "
                        f"{MIN_STACK_INPUT_FRAMES}-{MAX_STACK_INPUT_FRAMES}."
                    )
                if too_many_in_burst:
                    print(
                        "    - Reason: Burst safety check failed (likely a focus bracket)."
                    )
                if inputs_not_all_raw_backed:
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
                    counter, chosen = pick_unique_basenames_for_stem(
                        dest_dir, out_files, args.force, args.dry_run
                    )
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

                    success, _ = safe_file_operation(
                        "move",
                        orig_jpg_path,
                        dest_path,
                        "renaming",
                        args.force,
                        args.dry_run,
                    )
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
                            if raw_record
                            else file_db[input_stem]["files"].get("jpg")
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
                        ensure_directory_once(
                            lightroom_dest_dir, created_dirs, args.dry_run
                        )

                        stem_files = {
                            "jpg": file_db[input_stem]["files"].get("jpg"),
                            "raw": file_db[input_stem]["files"].get("raw"),
                        }
                        stem_files = {k: v for k, v in stem_files.items() if v}
                        counter, chosen = pick_unique_basenames_for_stem(
                            lightroom_dest_dir, stem_files, args.force, args.dry_run
                        )
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

                        for file_type in ["jpg", "raw"]:
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

        # Execute collected moves for lightroom-only mode
        if move_operations:
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
                    for future in as_completed(future_to_op):
                        orig_name, ldest, inp_stem = future_to_op[future]
                        try:
                            if future.result():
                                moved_input_count += 1
                                input_dest_dirs.add(ldest)
                                successful_moves_per_stem[inp_stem] += 1
                                if args.verbose:
                                    print(
                                        f"Moved input file '{orig_name}' to '{display_path(ldest)}'"
                                    )
                            else:
                                failed_count += 1
                        except Exception as e:
                            print(f"Error moving file '{orig_name}': {e}")
                            failed_count += 1
            else:
                for (
                    src_path,
                    dest_path,
                    desc,
                    orig_name,
                    ldest,
                    inp_stem,
                ) in move_operations:
                    success, _ = safe_file_operation(
                        "move", src_path, dest_path, desc, args.force, args.dry_run
                    )
                    if success:
                        moved_input_count += 1
                        input_dest_dirs.add(ldest)
                        successful_moves_per_stem[inp_stem] += 1
                        if args.verbose or args.dry_run:
                            print(
                                f"{'Would move' if args.dry_run else 'Moved'} input file '{orig_name}' to '{display_path(ldest)}'"
                            )
                    else:
                        failed_count += 1

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

            for data in file_db.values():
                if data.get("has_jpg") and not data.get("has_raw"):
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
                for data in file_db.values():
                    # ... (logic to submit jobs)
                    if data.get("has_jpg") and not data.get("has_raw"):
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
                        success = job["future"].result()
                        if success:
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
            for data in file_db.values():
                if data.get("has_jpg") and not data.get("has_raw"):
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
        total_moved = moved_output_count + moved_input_count + remaining_moved_count
        if args.dry_run:
            print(
                f"DRY RUN complete. {total_moved} files would be {past_tense_verb.lower()}."
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
            print(
                f"Done. {import_action} {total_moved} files in {exec_elapsed_time:.1f}s. "
                f"{source_note}"
                f"Breakdown: {moved_output_count} stacked outputs, {moved_input_count} stack inputs, {remaining_moved_count} remaining. "
                f"{throughput_info}Failures: {failed_count}."
            )
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
                f"DRY RUN: Would move {moved_input_count} input files (JPG and ORF) to:"
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
            print(f"Moved {moved_input_count} input files (JPG and ORF) to:")
            for d in sorted(input_dest_dirs):
                print(f"  - {d}")
        elif operation_mode == "stackcopy":
            print(
                f"\nDone. Copied and renamed {processed_count} JPG files{prefix_info} without corresponding raw files to the '{dest_dir}' directory."
            )
        else:  # copy mode
            action_desc = "Copied and renamed" if args.prefix else "Copied"
            print(
                f"\nDone. {action_desc} {processed_count} JPG files{prefix_info}{date_info} without corresponding raw files to '{dest_dir}'."
            )

    if skipped_count > 0:
        print(f"Skipped {skipped_count} files that were already processed.")

    if failed_count > 0:
        print(f"Failed to process {failed_count} files.")

    # Return non-zero status if any execution failures occurred (excluding dry-run)
    if not args.dry_run and failed_count > 0:
        if args.lightroomimport is not None and partial_failures_found:
            print(
                "\nWARNING: Some stems partially failed. Check logs above for details."
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
