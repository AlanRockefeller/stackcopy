#!/usr/bin/python3
# SPDX-License-Identifier: MIT

# Stackcopy version 1.5.2 by Alan Rockefeller
# January 31, 2026

# Copies / renames only the photos that have been stacked in-camera - designed for Olympus / OM System, though it might work for other cameras too.

from __future__ import annotations

import sys
import os
import shutil
import uuid
import argparse
import re
import errno
from bisect import bisect_left
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from typing import Any

LIGHTROOM_BASE_DIR = "/home/alan/pictures/olympus.stack.input.photos/"

# Regex to identify numeric stems for sequence grouping.
# It assumes numeric parts are 6 or more digits, common for Olympus/OM System in-camera stacking.
# Stems with fewer digits (e.g., 4-digit counters) will not be treated as numeric sequences.

NUMERIC_STEM_REGEX = re.compile(r"([a-zA-Z0-9_-]*)(\d{6,})")


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
    dest_dir: str, files_by_type: dict[str, Any], force: bool, _dry_run: bool
) -> tuple[int, dict[str, str]]:
    """
    Choose a single counter for *all* files in this stem (e.g., JPG+ORF) so they stay paired.
    Returns (counter, {file_type: chosen_basename}).
    Only applies counter suffixing when a destination collision exists and --force is NOT set.
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
    # If destination exists and we're not forcing, see if it's identical to the source.
    if os.path.exists(dest_path) and not force:
        # Check self-heal first (applies to both dry-run and real)
        is_self_heal = False
        try:
            if os.path.exists(src_path):
                src_size = os.path.getsize(src_path)
                dst_size = os.path.getsize(dest_path)
                # Self-heal: if destination is a 0-byte placeholder but source is non-zero
                if src_size > 0 and dst_size == 0:
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
            return False
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
                    return True
                else:
                    print(
                        f"Note: destination '{dest_path}' already exists with identical content; "
                        f"skipping copy from '{src_path}'."
                    )
                    return True

            print(f"Warning: '{dest_path}' already exists. Use --force to overwrite.")
            return False
        else:
            print(f"Warning: '{dest_path}' already exists. Use --force to overwrite.")
            return False

    if dry_run:
        return True

    try:
        if operation == "move":
            try:
                # Same-filesystem fast path
                os.replace(src_path, dest_path)
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
                else:
                    raise
        elif operation == "copy":
            _atomic_copy2(src_path, dest_path)

        return True

    except (OSError, shutil.Error) as e:
        print(f"Error {operation_name} '{src_path}' to '{dest_path}': {e}")
        return False


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

        # Threshold: max(2 GiB, 5% of total)
        reserve_bytes = max(2 * 1024**3, int(total_bytes * 0.05))

        estimated_free = free_bytes - req_bytes

        is_low = (req_bytes > free_bytes) or (estimated_free < reserve_bytes)

        if is_low:
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
                print(
                    "Refusing to proceed: destination space is low and no TTY is available to confirm."
                )
                sys.exit(1)

            response = input("  Proceed anyway? [y/N] ").strip().lower()
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
        return f"{action} '{filename}' to '{dest_filename}' in '{dest_dir}'"


def main():
    """Main program entry point."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Process JPG files without corresponding raw files"
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
        help="Same as --lightroom, but moves remaining files to ~/pictures/Lightroom/YEAR/DATE/.",
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
        action="store_true",
        help="Enable detailed diagnostic output for stack detection",
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
    args = parser.parse_args()

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

    if (args.lightroom or args.lightroomimport) and not args.dry_run and args.jobs == 1:
        # If user didn't explicitly request more jobs, pick something sensible
        # 4 workers max, but don't exceed 2x CPU cores
        auto_jobs = min(4, cpu_count * 2)
        if args.verbose:
            print(f"Auto-selecting {auto_jobs} worker jobs for Lightroom mode.")
        args.jobs = auto_jobs

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

    # Date filters should only work with copy operations (copy or stackcopy), not rename
    if (args.today or args.yesterday or args.date) and args.rename is not None:
        parser.error(
            "The --today, --yesterday, and --date arguments cannot be used with --rename operation."
        )

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

        # Ensure the Lightroom base directory exists
        try:
            ensure_directory_once(LIGHTROOM_BASE_DIR, created_dirs, args.dry_run)
        except OSError as e:
            print(
                f"Error creating Lightroom base directory '{LIGHTROOM_BASE_DIR}': {e}"
            )
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

    # Define a list of common raw photo extensions
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

    # --- 1. Scan directory and build file database ---
    # file_db stores metadata for each unique file stem found in the source directory.
    # Structure:
    # {
    #   "filename_stem": {
    #     "files": {
    #       "raw": {"path": "...", "mtime": datetime_obj, "date": date_obj},
    #       "jpg": {"path": "...", "mtime": datetime_obj, "date": date_obj}
    #     },
    #     "has_raw": bool,
    #     "has_jpg": bool,
    #     "numeric": {
    #       "prefix": "alpha_prefix",
    #       "num": int_sequence_number,
    #       "width": digit_width
    #     }
    #   },
    #   ...
    # }
    file_db = {}
    try:
        for entry in os.scandir(src_dir):
            if not entry.is_file():
                continue

            stem, ext = os.path.splitext(entry.name)
            ext_lower = ext.lower()

            record = file_db.setdefault(
                stem, {"files": {}, "has_raw": False, "has_jpg": False}
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

    except OSError as e:
        print(f"Error scanning source directory '{src_dir}': {e}")
        sys.exit(1)

    sequences_by_prefix = {}
    for stem, record in file_db.items():
        numeric_info = record.get("numeric")
        if numeric_info and (record.get("has_raw") or record.get("has_jpg")):
            sequences_by_prefix.setdefault(numeric_info["prefix"], []).append(
                (numeric_info["num"], stem)
            )
    for prefix in sequences_by_prefix:
        sequences_by_prefix[prefix].sort()

    # --- 2. Process files based on operation mode ---
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    moved_input_count = 0
    moved_output_count = 0
    stack_outputs_seen = 0  # number of stacked JPG outputs processed in Lightroom mode
    remaining_moved_count = 0

    if args.lightroom is not None or args.lightroomimport is not None:
        # --- Lightroom Mode ---
        input_dest_dirs = set()
        import_dest_dirs = set()
        # For printing collision renames only once per stem per destination.
        collision_notified = set()  # tuples like (dest_dir, stem_label)
        # Find stacked output files (JPG without ORF)
        stacked_outputs = set()
        for stem, data in file_db.items():
            # Any JPG without a corresponding RAW is considered a stacked output,
            # even if it has already been renamed with "stacked" in the filename.
            if data.get("has_jpg") and not data.get("has_raw"):
                jpg_record = data["files"].get("jpg")
                if not jpg_record:
                    continue

                if target_date:
                    file_date = get_file_date(jpg_record, args.verbose)
                    if file_date is None or file_date != target_date:
                        continue

                stacked_outputs.add(stem)

        claimed_input_stems = (
            set()
        )  # Stems claimed by a stack (to prevent reuse in logic)
        processed_stems_for_remaining = (
            set()
        )  # Stems that have been successfully queued/moved (for "remaining" logic)

        # MAX_STACK_GAP_SECONDS = 20 # Deprecated, replaced by split thresholds below
        MAX_OUTPUT_LAG_SECONDS = (
            120  # Allow time for camera to stack and save (Output -> Input 1)
        )
        MAX_INPUT_GAP_SECONDS = (
            6  # Tight gap between consecutive inputs (Input N -> Input N+1)
        )
        MAX_BURST_GAP_SECONDS = 2.0
        BURST_EXTRA_FRAMES_REQUIRED = 3
        move_operations = (
            []
        )  # List of (src, dest, description, orig_name_for_logging, dest_dir_for_logging)
        expected_moves_per_stem = defaultdict(
            int
        )  # stem -> int count of expected file moves
        successful_moves_per_stem = defaultdict(
            int
        )  # stem -> int count of confirmed successful moves

        for output_stem in sorted(stacked_outputs, reverse=True):
            # If this stem was already claimed as an input by a later stack (because we process in reverse),
            # it cannot be an output.
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

            # We defer the output move/rename until AFTER we validate the stack logic.
            # This matches Requirement B: "Output move must happen only after stack is accepted"

            numeric_info = output_data.get("numeric")
            if not numeric_info:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output JPG has no numeric stem.")
                continue

            prefix = numeric_info["prefix"]
            output_num = numeric_info["num"]
            sequence = sequences_by_prefix.get(prefix)

            if args.debug_stacks:
                print(f"  - Numeric Stem Info: prefix='{prefix}', number={output_num}")

            if not sequence:
                if args.debug_stacks:
                    print("  - Stack REJECTED: No sequence found for this prefix.")
                continue

            # Find potential input RAWs by scanning backwards from the output's number
            idx = bisect_left(sequence, (output_num, ""))
            if idx == 0:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output is the first in its sequence.")
                continue

            potential_inputs = []
            expected_num = output_num - 1
            current_index = idx - 1
            hit_input_cap = False
            stop_reason = "None"

            if args.debug_stacks:
                print("  - Scanning for Input frames (backward from output number):")

            # Initialize gap checking logic
            # The gap between the Output file and the first Input file (Input 1) can be large (camera processing time).
            # The gap between subsequent inputs (Input 1 -> Input 2) must be small (burst speed).
            prev_mtime = output_mtime
            if output_mtime is None:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output mtime is missing.")
                continue

            allowed_gap = MAX_OUTPUT_LAG_SECONDS
            gap_type = "output_lag"

            while current_index >= 0 and len(potential_inputs) < 15:
                candidate_num, candidate_stem = sequence[current_index]
                candidate_record = file_db[candidate_stem]

                if candidate_num != expected_num:
                    stop_reason = f"Number mismatch (expected {expected_num}, found {candidate_num})"
                    if args.debug_stacks:
                        print(
                            f"    - Input '{candidate_stem}': REJECTED ({stop_reason})"
                        )
                    break

                if candidate_stem in claimed_input_stems:
                    stop_reason = "Already claimed by another stack"
                    if args.debug_stacks:
                        print(
                            f"    - Input '{candidate_stem}': REJECTED ({stop_reason})"
                        )
                    break

                # Requirement A: "A stem is eligible if it has jpg OR raw"
                if not (
                    candidate_record.get("has_raw") or candidate_record.get("has_jpg")
                ):
                    # This shouldn't happen given how we build sequences, but good safety
                    stop_reason = "No corresponding RAW or JPG file found"
                    if args.debug_stacks:
                        print(
                            f"    - Input '{candidate_stem}': REJECTED ({stop_reason})"
                        )
                    break

                # Use get_stem_mtime to robustly get time from RAW or JPG
                input_mtime = get_stem_mtime(candidate_record, args.verbose)

                # Determine source for logging - check if we actually got a RAW mtime
                has_valid_raw_mtime = False
                if candidate_record.get("has_raw"):
                    raw_mtime_val = get_file_mtime(
                        candidate_record["files"]["raw"], False
                    )  # don't verbose log here
                    if raw_mtime_val:
                        has_valid_raw_mtime = True

                mtime_source = "RAW" if has_valid_raw_mtime else "JPG"

                # Time Gap Logic with Split Thresholds
                # Safety: If we don't have valid mtimes, we can't validate the stack timing.
                if not input_mtime or not prev_mtime:
                    time_gap = float("inf")
                else:
                    time_gap = abs((prev_mtime - input_mtime).total_seconds())

                if time_gap > allowed_gap:
                    stop_reason = f"Time gap too large ({time_gap:.2f}s > {allowed_gap}s, type: {gap_type})"
                    if args.debug_stacks:
                        print(
                            f"    - Input '{candidate_stem}': REJECTED ({stop_reason})"
                        )
                    break

                if args.debug_stacks:
                    print(
                        f"    - Input '{candidate_stem}': ACCEPTED (mtime source: {mtime_source}, {gap_type} gap={time_gap:.2f}s <= {allowed_gap}s)"
                    )

                # Update for next iteration
                # After the first accepted input, we check the gap between inputs, which must be tight.
                prev_mtime = input_mtime
                allowed_gap = MAX_INPUT_GAP_SECONDS
                gap_type = "input_gap"

                potential_inputs.append(candidate_stem)
                expected_num -= 1
                current_index -= 1

            if len(potential_inputs) == 15:
                hit_input_cap = True
                if args.debug_stacks:
                    print("  - Note: Reached 15-frame input cap.")

            # Safety check for focus-bracketing bursts (Requirement C)
            # Only apply burst safety when we hit the input cap (15 frames)
            too_many_in_burst = False
            if potential_inputs and hit_input_cap:
                # Probe further backward for BURST_EXTRA_FRAMES_REQUIRED additional CONSECUTIVE numbers
                burst_probe_stems = []
                probe_index = current_index
                probe_expected_num = expected_num

                # Try to recruit extra frames
                while (
                    probe_index >= 0
                    and len(burst_probe_stems) < BURST_EXTRA_FRAMES_REQUIRED
                ):
                    probe_num, probe_stem = sequence[probe_index]

                    if probe_num != probe_expected_num:
                        # Not consecutive, so not part of this burst
                        break

                    burst_probe_stems.append(probe_stem)
                    probe_expected_num -= 1
                    probe_index -= 1

                if len(burst_probe_stems) >= BURST_EXTRA_FRAMES_REQUIRED:
                    # We found enough extra consecutive frames. Now check their timing vs the FIRST input frame.
                    # potential_inputs is ordered [output-1, output-2 ...], so the "first" (oldest) input is the last element.
                    first_input_stem = potential_inputs[-1]
                    first_input_mtime = get_stem_mtime(
                        file_db[first_input_stem], args.verbose
                    )

                    # We only care if ALL probe frames are within the tight burst gap
                    all_in_burst_gap = True
                    for probe_stem in burst_probe_stems:
                        probe_mtime = get_stem_mtime(file_db[probe_stem], args.verbose)
                        # If we can't get mtime, we can't prove it's a burst, so assume safe
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

            # Final decision on the stack
            is_valid_stack = (
                3 <= len(potential_inputs) <= 15
            ) and not too_many_in_burst

            if args.debug_stacks:
                print(
                    f"  - Final Decision: {'ACCEPTED' if is_valid_stack else 'REJECTED'}"
                )
                if not (3 <= len(potential_inputs) <= 15):
                    print(
                        f"    - Reason: Found {len(potential_inputs)} inputs, but requires 3-15."
                    )
                if too_many_in_burst:
                    print(
                        "    - Reason: Burst safety check failed (likely a focus bracket)."
                    )
                print("--- End Debugging Stack ---")

            if is_valid_stack:
                # Requirement E: Update claimed_input_stems immediately to prevent reuse
                # We do this BEFORE attempting the output move. If the output move fails later,
                # these inputs remain "claimed" internally, effectively binding them to this
                # (failed) stack rather than letting them float to a secondary match. This is safer.
                for input_stem in potential_inputs:
                    claimed_input_stems.add(input_stem)

                # --- Execute Output Move/Rename IMMEDIATELY (Requirement B) ---
                output_move_success = False

                # 1. Rename output to "stacked" in place
                if not is_already_processed(output_filename):
                    stem_only, ext = os.path.splitext(output_filename)
                    new_filename = create_new_filename(stem_only, ext, args.prefix)
                    # Collision-safe in-place rename (rare, but can happen across repeated runs/sessions)
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

                    if safe_file_operation(
                        "move",
                        orig_jpg_path,
                        dest_path,
                        "renaming",
                        args.force,
                        args.dry_run,
                    ):
                        jpg_record.update(
                            {
                                "path": dest_path,
                                "basename": os.path.basename(dest_path),
                                "entry": None,
                            }
                        )
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
                        # If output rename fails, we probably shouldn't move inputs, but practically we can continue if only rename failed.
                        # But strictly, if we can't rename, the stack state is messy.
                else:
                    output_move_success = True  # Already renamed

                # 2. Move output to Lightroom import folder (if requested)
                successfully_moved_output_to_import = False
                if args.lightroomimport and output_move_success:
                    file_date = get_file_date(jpg_record, args.verbose)
                    if file_date:
                        lightroom_import_base_dir = os.path.expanduser(
                            "~/pictures/Lightroom"
                        )
                        dest_dir_import = os.path.join(
                            lightroom_import_base_dir,
                            str(file_date.year),
                            file_date.strftime("%Y-%m-%d"),
                        )
                        ensure_directory_once(
                            dest_dir_import, created_dirs, args.dry_run
                        )

                        current_basename = jpg_record["basename"]
                        # Collision-safe move into Lightroom import folder
                        out_files = {
                            "jpg": {
                                "basename": current_basename,
                                "path": jpg_record["path"],
                            }
                        }
                        counter, chosen = pick_unique_basenames_for_stem(
                            dest_dir_import, out_files, args.force, args.dry_run
                        )
                        chosen_name = chosen.get("jpg", current_basename)
                        dest_path = os.path.join(dest_dir_import, chosen_name)
                        if counter > 1:
                            key = (dest_dir_import, current_basename)
                            if key not in collision_notified:
                                collision_notified.add(key)
                                print_collision_rename_notice(
                                    dest_dir_import,
                                    current_basename,
                                    [(current_basename, chosen_name)],
                                    args.dry_run,
                                )

                        if safe_file_operation(
                            "move",
                            jpg_record["path"],
                            dest_path,
                            "moving stacked output",
                            args.force,
                            args.dry_run,
                        ):
                            jpg_record.update(
                                {
                                    "path": dest_path,
                                    "basename": os.path.basename(dest_path),
                                    "entry": None,
                                }
                            )
                            moved_output_count += 1
                            import_dest_dirs.add(dest_dir_import)
                            if args.verbose or args.dry_run:
                                print(
                                    f"{ 'Would move' if args.dry_run else 'Moved'} stacked output '{os.path.basename(dest_path)}' to '{dest_dir_import}'"
                                )
                            successfully_moved_output_to_import = True
                        else:
                            failed_count += 1

                # Mark output as "processed" for remaining files logic
                # If lightroomimport: only if import move succeeded.
                # If lightroom: only if rename succeeded.
                if args.lightroomimport:
                    if successfully_moved_output_to_import:
                        processed_stems_for_remaining.add(output_stem)
                elif output_move_success:
                    processed_stems_for_remaining.add(output_stem)

                # --- Queue Input Moves (Only if output move succeeded) ---
                if output_move_success:
                    for input_stem in potential_inputs:
                        raw_record = file_db[input_stem]["files"].get(
                            "raw"
                        )  # Try raw first for date
                        date_record = (
                            raw_record
                            if raw_record
                            else file_db[input_stem]["files"].get("jpg")
                        )
                        if not date_record:
                            continue  # Should not happen

                        file_date = get_file_date(date_record, args.verbose)
                        if not file_date:
                            if args.verbose:
                                print(
                                    f"Warning: Could not determine date for '{input_stem}', skipping move."
                                )
                            continue

                        lightroom_dest_dir = os.path.join(
                            LIGHTROOM_BASE_DIR,
                            str(file_date.year),
                            file_date.strftime("%Y-%m-%d"),
                        )
                        ensure_directory_once(
                            lightroom_dest_dir, created_dirs, args.dry_run
                        )

                        # Collision-safe naming: if destination already has Pxxxxx.JPG/ORF from an earlier import,
                        # rename both JPG+ORF with a shared __N counter to avoid overwriting.
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
                                    # Update basename so later logic (including remaining-file pass) stays consistent.
                                    fi["basename"] = new
                            key = (lightroom_dest_dir, input_stem)
                            if key not in collision_notified:
                                collision_notified.add(key)
                                print_collision_rename_notice(
                                    lightroom_dest_dir,
                                    input_stem,
                                    changes,
                                    args.dry_run,
                                )

                        # Don't mark as processed yet - wait until confirmed success
                        # processed_stems_for_remaining.add(input_stem)

                        for file_type in ["jpg", "raw"]:
                            file_info = file_db[input_stem]["files"].get(file_type)
                            if file_info:
                                src_path = file_info["path"]
                                # Safety: Verify file exists before queuing to avoid ghost failures
                                if not os.path.exists(src_path):
                                    if args.verbose:
                                        print(
                                            f"Warning: File '{src_path}' missing at queue time, skipping."
                                        )
                                    continue

                                dest_path = os.path.join(
                                    lightroom_dest_dir, file_info["basename"]
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
                                # Track expected moves for this stem
                                expected_moves_per_stem[input_stem] += 1

        # --- Execute collected moves ---
        if move_operations:
            # Check disk space before executing moves
            ops_for_check = [(op[0], op[1], "move") for op in move_operations]
            confirm_if_low_space(ops_for_check, args.dry_run)

            if args.jobs > 1 and not args.dry_run:
                with ThreadPoolExecutor(max_workers=args.jobs) as executor:
                    # Submit all move operations to the thread pool
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
                    # Process results as they complete
                    # Process results as they complete
                    for future in as_completed(future_to_op):
                        orig_name, ldest, inp_stem = future_to_op[future]
                        try:
                            if future.result():
                                # Increment counter if successful
                                moved_input_count += 1

                                input_dest_dirs.add(ldest)
                                successful_moves_per_stem[inp_stem] += 1
                                if args.verbose:
                                    print(
                                        f"Moved input file '{orig_name}' to '{ldest}'"
                                    )
                            else:
                                failed_count += (
                                    1  # Count failures from parallel execution
                                )
                        except Exception as e:
                            print(f"Error moving file '{orig_name}': {e}")
                            failed_count += 1
            else:  # Sequential move for single job or dry run
                for (
                    src_path,
                    dest_path,
                    desc,
                    orig_name,
                    ldest,
                    inp_stem,
                ) in move_operations:
                    if safe_file_operation(
                        "move", src_path, dest_path, desc, args.force, args.dry_run
                    ):
                        moved_input_count += 1
                        input_dest_dirs.add(ldest)
                        successful_moves_per_stem[inp_stem] += 1
                        if args.verbose or args.dry_run:
                            print(
                                f"{ 'Would move' if args.dry_run else 'Moved'} input file '{orig_name}' to '{ldest}'"
                            )
                    else:
                        failed_count += 1  # Count failures from sequential execution

        # Post-process validation: Only mark input stems as "Processed" if ALL their files moved successfully
        for stem, expected_count in expected_moves_per_stem.items():
            if successful_moves_per_stem[stem] == expected_count:
                processed_stems_for_remaining.add(stem)
            elif args.verbose:
                print(
                    f"Warning: Stem '{stem}' had partial move failure, leaving for 'remaining files' logic."
                )

        if args.lightroomimport is not None:
            print(
                f"\n{'DRY RUN: ' if args.dry_run else ''}Lightroom Import: Moving remaining files..."
            )
            lightroom_import_base_dir = os.path.expanduser("~/pictures/Lightroom")

            all_stems = set(file_db.keys())
            # Treat both successfully moved input stems and accepted output stems as "already handled"
            # Explicitly do NOT include all 'stacked_outputs' candidates indiscriminately,
            # as rejected candidates should be moved as remaining files.

            # Using our new safely tracked set:
            processed_stems = processed_stems_for_remaining
            remaining_stems = all_stems - processed_stems

            # --- Pre-flight disk check for remaining files ---
            ops_check_list = []
            for stem in remaining_stems:
                record = file_db[stem]
                for file_type in ["jpg", "raw"]:
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
                    # We treat this as a move. Destination path is just directory + basename (approx is fine)
                    ops_check_list.append(
                        (
                            src_path,
                            os.path.join(dest_dir_import, file_info["basename"]),
                            "move",
                        )
                    )

            confirm_if_low_space(ops_check_list, args.dry_run)
            # --- End pre-flight check ---

            for stem in sorted(remaining_stems):
                record = file_db[stem]

                # Group files by their destination directory (usually the same, but safety first)
                files_by_dest = defaultdict(list)
                for file_type in ["jpg", "raw"]:
                    file_info = record["files"].get(file_type)
                    if not file_info:
                        continue

                    src_path = file_info["path"]
                    if not os.path.exists(src_path):
                        continue

                    file_date = get_file_date(file_info, args.verbose)
                    if file_date is None:
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

                # Process moves for each destination directory
                for dest_dir_import, files in files_by_dest.items():
                    ensure_directory_once(dest_dir_import, created_dirs, args.dry_run)

                    # Handle collisions once per (stem, destination)
                    stem_files_for_dest = dict(files)
                    counter, chosen = pick_unique_basenames_for_stem(
                        dest_dir_import, stem_files_for_dest, args.force, args.dry_run
                    )
                    if counter > 1:
                        changes = []
                        for ft, fi in stem_files_for_dest.items():
                            old = fi["basename"]
                            new = chosen.get(ft, old)
                            if new != old:
                                changes.append((old, new))
                                fi["basename"] = new
                        key = (dest_dir_import, stem)
                        if key not in collision_notified:
                            collision_notified.add(key)
                            print_collision_rename_notice(
                                dest_dir_import, stem, changes, args.dry_run
                            )

                    for _, file_info in files:
                        dest_path = os.path.join(dest_dir_import, file_info["basename"])
                        if safe_file_operation(
                            "move",
                            file_info["path"],
                            dest_path,
                            "moving remaining file",
                            args.force,
                            args.dry_run,
                        ):
                            file_info.update(
                                {
                                    "path": dest_path,
                                    "basename": os.path.basename(dest_path),
                                    "entry": None,
                                }
                            )
                            remaining_moved_count += 1
                            import_dest_dirs.add(dest_dir_import)
                            if args.verbose or args.dry_run:
                                print(
                                    f"{ 'Would move' if args.dry_run else 'Moved'} remaining file '{file_info['basename']}' to '{dest_dir_import}'"
                                )
                        else:
                            failed_count += 1

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
                    except (
                        Exception
                    ) as e:
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
                        success = safe_file_operation(
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
                        success = safe_file_operation(
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
                        success = safe_file_operation(
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

    if args.dry_run:
        # Custom summary for dry-run
        if operation_mode == "rename":
            print(
                f"\nDRY RUN: Would rename {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'."
            )
        elif operation_mode == "lightroom" or operation_mode == "lightroomimport":
            print(
                f"\nDRY RUN: Would process {stack_outputs_seen} stacked JPG files"
                f"{prefix_info} in '{src_dir}' (renaming {processed_count} of them)."
            )
            print(
                f"DRY RUN: Would move {moved_input_count} input files (JPG and ORF) to:"
            )
            for d in sorted(input_dest_dirs):
                print(f"  - {d}")

            if operation_mode == "lightroomimport":
                print(
                    f"DRY RUN: Would move {moved_output_count} stacked output files to:"
                )
                for d in sorted(import_dest_dirs):
                    print(f"  - {d.replace(os.path.expanduser('~'), '~')}")
                print(
                    f"DRY RUN: Would move {remaining_moved_count} remaining files to:"
                )
                for d in sorted(import_dest_dirs):
                    print(f"  - {d.replace(os.path.expanduser('~'), '~')}")
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
        elif operation_mode == "lightroom" or operation_mode == "lightroomimport":
            print(
                f"\nDone. Processed {stack_outputs_seen} stacked JPG files"
                f"{prefix_info} in '{src_dir}' (renamed {processed_count})."
            )
            print(f"Moved {moved_input_count} input files (JPG and ORF) to:")
            for d in sorted(input_dest_dirs):
                print(f"  - {d}")

            if operation_mode == "lightroomimport":
                print(f"Moved {moved_output_count} stacked output files to:")
                for d in sorted(import_dest_dirs):
                    print(f"  - {d.replace(os.path.expanduser('~'), '~')}")
                print(f"Moved {remaining_moved_count} remaining files to:")
                for d in sorted(import_dest_dirs):
                    print(f"  - {d.replace(os.path.expanduser('~'), '~')}")
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


if __name__ == "__main__":
    main()
