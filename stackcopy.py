#!/usr/bin/python3
# SPDX-License-Identifier: MIT

# Stackcopy version 1.4 by Alan Rockefeller
# January 4, 2026

# Copies / renames only the photos that have been stacked in-camera - designed for Olympus / OM System, though it might work for other cameras too.

import sys
import os
import shutil
import uuid
import argparse
import re
import errno
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta

LIGHTROOM_BASE_DIR = "/home/alan/pictures/olympus.stack.input.photos/"
# Regex to identify numeric stems for sequence grouping.
# It assumes numeric parts are 6 or more digits, common for Olympus/OM System in-camera stacking.
# Stems with fewer digits (e.g., 4-digit counters) will not be treated as numeric sequences.
NUMERIC_STEM_REGEX = re.compile(r'([a-zA-Z0-9_-]*)(\d{6,})')

def get_file_mtime(file_record, verbose=False):
    """Lazily fetch mtime, caching the result."""
    if file_record.get('mtime') is not None:
        return file_record['mtime']
    if 'entry' in file_record:
        try:
            stat_info = file_record['entry'].stat(follow_symlinks=False)
            mtime_dt = datetime.fromtimestamp(stat_info.st_mtime)
            file_record['mtime'] = mtime_dt
            file_record['date'] = mtime_dt.date()
            return mtime_dt
        except OSError as e:
            if verbose:
                print(f"Warning: Could not stat file '{file_record['path']}': {e}")
            pass
    return None

def get_file_date(file_record, verbose=False):
    """Lazily fetch date, caching the result."""
    if file_record.get('date') is not None:
        return file_record['date']
    # Calling get_file_mtime will populate both mtime and date
    if get_file_mtime(file_record, verbose):
        return file_record.get('date')
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
    new_stem = ' '.join(parts)
    # Collapse multiple spaces into single spaces
    new_stem = re.sub(r'\s+', ' ', new_stem)
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
    stem, ext = os.path.splitext(filename)
    # Use word boundary regex to match 'stacked' as a complete word
    # This will match: "image stacked.jpg", "stacked_image.jpg", "stacked-photo.jpg", etc.
    return bool(re.search(r'\bstacked\b', stem.lower()))

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


def _atomic_copy2(src_path: str, dest_path: str) -> None:
    """Copy to a temp file in dest dir, then atomically replace dest_path."""
    dest_dir = os.path.dirname(dest_path)
    os.makedirs(dest_dir, exist_ok=True)
    tmp_path = os.path.join(dest_dir, f".__stackcopy_tmp__{os.path.basename(dest_path)}.{uuid.uuid4().hex}")
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

def safe_file_operation(operation, src_path, dest_path, operation_name, force=False, dry_run=False):
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
            msg = "replacing from source" if not dry_run else "would replace from source"
            print(f"Note: destination '{dest_path}' exists but is 0 bytes; {msg}.")
            force = True
        elif dry_run:
            print(f"Warning: '{dest_path}' already exists. Would need --force to overwrite.")
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


def is_cross_device(src_path, dest_path):
    """Check if source and destination are on different devices."""
    try:
        return os.stat(src_path).st_dev != os.stat(dest_path).st_dev
    except OSError:
        return True  # Assume cross-device if we can't tell

def format_action_message(operation_mode, filename, dest_filename, dest_dir, success, dry_run, used_prefix):
    """Generate consistent action messages for all operations."""
    if operation_mode == "rename" or operation_mode == "lightroom" or operation_mode == "lightroomimport":
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
                action = "Copied and renamed" if success else "Failed to copy and rename"
        else:
            if dry_run:
                action = "Would copy"
            else:
                action = "Copied" if success else "Failed to copy"
        return f"{action} '{filename}' to '{dest_filename}' in '{dest_dir}'"

def main():
    """Main program entry point."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process JPG files without corresponding raw files")

    # Create a mutually exclusive group for the three operation modes
    mode_group = parser.add_mutually_exclusive_group()

    # Copy mode - requires source and destination
    mode_group.add_argument("--copy", nargs=2, metavar=('SRC_DIR', 'DEST_DIR'),
                            help="Copy JPG files without matching raw files from source to destination. Can be used with --prefix.")

    # Rename mode - optional directory argument (renames files in-place)
    mode_group.add_argument("--rename", "-r", nargs='?', const=os.getcwd(), metavar='DIR',
                            help="Rename JPG files without matching raw files in-place by adding ' stacked' (defaults to current directory)")

    # Stack copy mode - optional directory argument (copies to 'stacked' subdirectory)
    mode_group.add_argument("--stackcopy", nargs='?', const=os.getcwd(), metavar='DIR',
                            help="Copy JPG files without matching raw files to a 'stacked' subdirectory with ' stacked' added to filenames")

    # Lightroom mode - optional directory argument
    mode_group.add_argument("--lightroom", nargs='?', const=os.getcwd(), metavar='DIR',
                            help="Move input files (JPG and ORF) to a dated directory structure and rename the stacked JPG in place.")

    # Lightroom import mode - optional directory argument
    mode_group.add_argument("--lightroomimport", nargs='?', const=os.getcwd(), metavar='DIR',
                            help="Same as --lightroom, but moves remaining files to ~/pictures/Lightroom/YEAR/DATE/.")

    # Add date filtering options
    date_group = parser.add_argument_group('Date Filtering (optional, for copy operations)')
    date_group.add_argument("--today", action="store_true", help="Process JPGs created today that don't have a corresponding raw file.")
    date_group.add_argument("--yesterday", action="store_true", help="Process JPGs created yesterday that don't have a corresponding raw file.")
    date_group.add_argument("--date", metavar='YYYY-MM-DD', help="Process JPGs from a specific date that don't have a corresponding raw file.")

    # Add prefix option
    parser.add_argument("--prefix", metavar='PREFIX',
                        help="Add a custom prefix before ' stacked' in the filename when using --copy, --rename, or --stackcopy.")

    # Add dry-run option
    parser.add_argument("--dry", "--dry-run", dest="dry_run", action="store_true",
                        help="Show what would happen without making any actual changes")

    # Add verbose flag
    parser.add_argument("-v", "--verbose", action="store_true", help="Show detailed information about processed files")

    # Add overwrite protection option
    parser.add_argument("--force", action="store_true", help="Overwrite existing files without prompting")

    # Add debug flag for stack detection
    parser.add_argument("--debug-stacks", action="store_true", help="Enable detailed diagnostic output for stack detection")

    parser.add_argument("-j", "--jobs", type=int, default=1, metavar="N",
                        help="Number of parallel copy workers to use for --copy/--stackcopy (default: 1)")

    # Parse arguments
    args = parser.parse_args()

    if args.jobs < 1:
        parser.error("--jobs must be at least 1.")

    # Clamp number of jobs to a reasonable limit
    cpu_count = os.cpu_count() or 1
    if args.jobs > cpu_count * 2:
        if args.verbose:
            print(f"Warning: --jobs reduced from {args.jobs} to {cpu_count * 2} (2x CPU cores) to avoid resource exhaustion.")
        args.jobs = cpu_count * 2

    if (args.lightroom or args.lightroomimport) and not args.dry_run:
        # If user didn't explicitly request more jobs, pick something sensible
        if args.jobs == 1:
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
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"Error: Date format for --date must be YYYY-MM-DD. You provided '{args.date}'.")
            sys.exit(1)

    # Date filters should only work with copy operations (copy or stackcopy), not rename
    if (args.today or args.yesterday or args.date) and args.rename is not None:
        parser.error("The --today, --yesterday, and --date arguments cannot be used with --rename operation.")

    # If no operation mode is specified, show help and exit
    if not args.copy and args.rename is None and args.stackcopy is None and args.lightroom is None and args.lightroomimport is None:
        parser.print_help()
        sys.exit(1)

    # Determine operation mode and set directories
    if args.copy:
        operation_mode = "copy"
        src_dir = normalize_path(args.copy[0])
        dest_dir = normalize_path(args.copy[1])

        # Verify that the source directory exists
        if not os.path.isdir(src_dir):
            print(f"Error: Source directory '{src_dir}' does not exist or is not a directory.")
            sys.exit(1)

        # Check if source and destination are the same
        if paths_are_same(src_dir, dest_dir):
            print(f"Error: Source and destination directories cannot be the same.")
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
            print(f"Error: Directory '{work_dir}' does not exist or is not a directory.")
            sys.exit(1)

        # For rename mode, source and working directory are the same
        src_dir = work_dir
        dest_dir = work_dir  # We're renaming in-place
    elif args.lightroom is not None or args.lightroomimport is not None: # --lightroom mode
        operation_mode = "lightroom" if args.lightroom is not None else "lightroomimport"
        work_dir = normalize_path(args.lightroom if args.lightroom is not None else args.lightroomimport)

        # Verify that the specified directory exists
        if not os.path.isdir(work_dir):
            print(f"Error: Directory '{work_dir}' does not exist or is not a directory.")
            sys.exit(1)

        # For lightroom mode, source and working directory are the same
        src_dir = work_dir
        dest_dir = work_dir  # We're renaming in-place

        # Ensure the Lightroom base directory exists
        try:
            ensure_directory_once(LIGHTROOM_BASE_DIR, created_dirs, args.dry_run)
        except OSError as e:
            print(f"Error creating Lightroom base directory '{LIGHTROOM_BASE_DIR}': {e}")
            sys.exit(1)
    else:  # --stackcopy mode
        operation_mode = "stackcopy"
        work_dir = normalize_path(args.stackcopy)

        # Verify that the specified directory exists
        if not os.path.isdir(work_dir):
            print(f"Error: Directory '{work_dir}' does not exist or is not a directory.")
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
    RAW_EXTENSIONS = {'.orf', '.cr2', '.nef', '.arw', '.dng', '.pef', '.rw2', '.raf', '.raw', '.sr2'}
    JPG_EXTENSIONS = {'.jpg', '.jpeg'}

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

            record = file_db.setdefault(stem, {'files': {}, 'has_raw': False, 'has_jpg': False})
            file_meta = {'path': entry.path, 'basename': entry.name, 'entry': entry, 'mtime': None, 'date': None} # Store entry object and basename

            if 'numeric' not in record:
                numeric_match = NUMERIC_STEM_REGEX.match(stem)
                if numeric_match:
                    prefix, num_str = numeric_match.groups()
                    record['numeric'] = {
                        'prefix': prefix,
                        'num': int(num_str),
                        'width': len(num_str)
                    }

            if ext_lower in RAW_EXTENSIONS:
                record['has_raw'] = True
                record['files']['raw'] = file_meta
            elif ext_lower in JPG_EXTENSIONS:
                record['has_jpg'] = True
                record['files']['jpg'] = file_meta

    except OSError as e:
        print(f"Error scanning source directory '{src_dir}': {e}")
        sys.exit(1)

    raw_sequences_by_prefix = {}
    for stem, record in file_db.items():
        numeric_info = record.get('numeric')
        if numeric_info and record.get('has_raw'):
            raw_sequences_by_prefix.setdefault(numeric_info['prefix'], []).append((numeric_info['num'], stem))
    for prefix in raw_sequences_by_prefix:
        raw_sequences_by_prefix[prefix].sort()


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
        # Find stacked output files (JPG without ORF)
        stacked_outputs = set()
        for stem, data in file_db.items():
            # Any JPG without a corresponding RAW is considered a stacked output,
            # even if it has already been renamed with "stacked" in the filename.
            if data.get('has_jpg') and not data.get('has_raw'):
                jpg_record = data['files'].get('jpg')
                if not jpg_record:
                    continue

                if target_date:
                    file_date = get_file_date(jpg_record, args.verbose)
                    if file_date is None or file_date != target_date:
                        continue

                stacked_outputs.add(stem)



        moved_stems = set()
        MAX_STACK_GAP_SECONDS = 20
        move_operations = [] # List of (src, dest, description, orig_name_for_logging, dest_dir_for_logging)


        for output_stem in sorted(stacked_outputs):
            output_data = file_db[output_stem]
            jpg_record = output_data['files'].get('jpg')
            if not jpg_record:
                continue

            stack_outputs_seen += 1

            orig_jpg_path = jpg_record['path']
            output_mtime = get_file_mtime(jpg_record, args.verbose)
            output_filename = os.path.basename(orig_jpg_path)

            if args.debug_stacks:
                print(f"\n--- Debugging Stack for Output: {output_filename} ---")
                print(f"  - Output JPG: '{output_filename}' (mtime: {output_mtime})")

            # Rename the stacked output if not already processed
            if not is_already_processed(output_filename):
                stem_only, ext = os.path.splitext(output_filename)
                new_filename = create_new_filename(stem_only, ext, args.prefix)
                dest_path = os.path.join(dest_dir, new_filename)

                if safe_file_operation("move", orig_jpg_path, dest_path, "renaming", args.force, args.dry_run):
                    jpg_record.update({'path': dest_path, 'basename': new_filename, 'entry': None})
                    processed_count += 1
                    if args.verbose or args.dry_run:
                        print(format_action_message(operation_mode, output_filename, new_filename, dest_dir, True, args.dry_run, bool(args.prefix)))
                else:
                    failed_count += 1
                    if args.debug_stacks:
                        print("  - Stack REJECTED: Failed to rename output JPG.")
                    continue
            
            if args.lightroomimport:
                file_date = get_file_date(jpg_record, args.verbose)
                if file_date:
                    lightroom_import_base_dir = os.path.expanduser("~/pictures/Lightroom")
                    dest_dir_import = os.path.join(
                        lightroom_import_base_dir,
                        str(file_date.year),
                        file_date.strftime('%Y-%m-%d')
                    )
                    ensure_directory_once(dest_dir_import, created_dirs, args.dry_run)
                    
                    # jpg_record['basename'] is updated if we renamed it above
                    current_basename = jpg_record['basename']
                    dest_path = os.path.join(dest_dir_import, current_basename)
                    
                    if safe_file_operation("move", jpg_record['path'], dest_path, "moving stacked output", args.force, args.dry_run):
                        jpg_record.update({'path': dest_path, 'basename': current_basename, 'entry': None})
                        moved_output_count += 1
                        if args.verbose or args.dry_run:
                             print(f"{ 'Would move' if args.dry_run else 'Moved'} stacked output '{current_basename}' to '{dest_dir_import}'")
                    else:
                        failed_count += 1

            numeric_info = output_data.get('numeric')
            if not numeric_info:
                if args.debug_stacks:
                    print("  - Stack REJECTED: Output JPG has no numeric stem.")
                continue

            prefix = numeric_info['prefix']
            output_num = numeric_info['num']
            raw_sequence = raw_sequences_by_prefix.get(prefix)

            if args.debug_stacks:
                print(f"  - Numeric Stem Info: prefix='{prefix}', number={output_num}")

            if not raw_sequence:
                if args.debug_stacks:
                    print("  - Stack REJECTED: No RAW sequence found for this prefix.")
                continue

            # Find potential input RAWs by scanning backwards from the output's number
            idx = bisect_left(raw_sequence, (output_num, ""))
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
                print("  - Scanning for Input RAWs (backward from output number):")

            while current_index >= 0 and len(potential_inputs) < 15:
                candidate_num, candidate_stem = raw_sequence[current_index]
                
                if candidate_num != expected_num:
                    stop_reason = f"Number mismatch (expected {expected_num}, found {candidate_num})"
                    if args.debug_stacks:
                        print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                    break
                
                if candidate_stem in moved_stems:
                    stop_reason = "Already moved as part of another stack"
                    if args.debug_stacks:
                        print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                    break

                candidate_raw = file_db[candidate_stem]['files'].get('raw')
                if not candidate_raw:
                    stop_reason = "No corresponding RAW file found"
                    if args.debug_stacks:
                        print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                    break

                input_mtime = get_file_mtime(candidate_raw, args.verbose)
                time_gap = abs((output_mtime - input_mtime).total_seconds()) if output_mtime and input_mtime else float('inf')

                if time_gap > MAX_STACK_GAP_SECONDS:
                    stop_reason = f"Time gap too large ({time_gap:.2f}s > {MAX_STACK_GAP_SECONDS}s)"
                    if args.debug_stacks:
                        print(f"    - Input '{candidate_stem}': REJECTED ({stop_reason})")
                    break

                if args.debug_stacks:
                    print(f"    - Input '{candidate_stem}': ACCEPTED (mtime: {input_mtime}, gap: {time_gap:.2f}s)")
                
                potential_inputs.append(candidate_stem)
                expected_num -= 1
                current_index -= 1

            if len(potential_inputs) == 15:
                hit_input_cap = True
                if args.debug_stacks:
                    print("  - Note: Reached 15-frame input cap.")

            # Safety check for focus-bracketing bursts
            too_many_in_burst = False
            if potential_inputs and hit_input_cap and current_index >= 0:
                prev_stem = raw_sequence[current_index][1]
                prev_raw = file_db[prev_stem]['files'].get('raw')
                first_input_mtime = get_file_mtime(file_db[potential_inputs[-1]]['files'].get('raw'), args.verbose)

                if prev_raw:
                    prev_mtime = get_file_mtime(prev_raw, args.verbose)
                    if first_input_mtime and prev_mtime and abs((first_input_mtime - prev_mtime).total_seconds()) <= MAX_STACK_GAP_SECONDS:
                        too_many_in_burst = True
                        if args.debug_stacks:
                            print("  - Burst Safety Check: TRIGGERED. Frame before stack start is too close in time.")

            # Final decision on the stack
            is_valid_stack = (3 <= len(potential_inputs) <= 15) and not too_many_in_burst
            
            if args.debug_stacks:
                print(f"  - Final Decision: {'ACCEPTED' if is_valid_stack else 'REJECTED'}")
                if not (3 <= len(potential_inputs) <= 15):
                    print(f"    - Reason: Found {len(potential_inputs)} inputs, but requires 3-15.")
                if too_many_in_burst:
                    print(f"    - Reason: Burst safety check failed (likely a focus bracket).")
                print("--- End Debugging Stack ---")


            if is_valid_stack:
                for input_stem in potential_inputs:
                    raw_record = file_db[input_stem]['files'].get('raw')
                    if not raw_record: continue
                    file_date = get_file_date(raw_record, args.verbose)
                    if not file_date:
                        if args.verbose: print(f"Warning: Could not determine date for '{input_stem}', skipping move.")
                        continue
                    
                    moved_stems.add(input_stem)
                    lightroom_dest_dir = os.path.join(LIGHTROOM_BASE_DIR, str(file_date.year), file_date.strftime('%Y-%m-%d'))
                    ensure_directory_once(lightroom_dest_dir, created_dirs, args.dry_run)

                    for file_type in ['jpg', 'raw']:
                        file_info = file_db[input_stem]['files'].get(file_type)
                        if file_info:
                            src_path = file_info['path']
                            dest_path = os.path.join(lightroom_dest_dir, file_info['basename'])
                            move_operations.append((src_path, dest_path, "moving input file", file_info['basename'], lightroom_dest_dir, input_stem))
        
        # --- Execute collected moves ---
        if move_operations:
            if args.jobs > 1 and not args.dry_run:
                with ThreadPoolExecutor(max_workers=args.jobs) as executor:
                    # Submit all move operations to the thread pool
                    future_to_op = {
                            executor.submit(
                                safe_file_operation, "move", src, dst, desc, args.force, args.dry_run
                            ): (orig_name, ldest, inp_stem)
                            for src, dst, desc, orig_name, ldest, inp_stem in move_operations
                    }
                    # Process results as they complete
                    for future in future_to_op:
                        orig_name, ldest, inp_stem = future_to_op[future]
                        try:
                            if future.result():
                                # Increment counter if successful
                                moved_input_count += 1
                                moved_stems.add(inp_stem)
                                if args.verbose:
                                    print(f"Moved input file '{orig_name}' to '{ldest}'")
                            else:
                                failed_count += 1 # Count failures from parallel execution
                        except Exception as e:
                            print(f"Error moving file '{orig_name}': {e}")
                            failed_count += 1
            else:  # Sequential move for single job or dry run
                for src_path, dest_path, desc, orig_name, ldest, inp_stem in move_operations:
                    if safe_file_operation("move", src_path, dest_path, desc, args.force, args.dry_run):
                        moved_input_count += 1
                        moved_stems.add(inp_stem)
                        if args.verbose or args.dry_run:
                            print(f"{ 'Would move' if args.dry_run else 'Moved'} input file '{orig_name}' to '{ldest}'")
                    else:
                        failed_count += 1 # Count failures from sequential execution



        if args.lightroomimport is not None:
            print(f"\n{'DRY RUN: ' if args.dry_run else ''}Lightroom Import: Moving remaining files...")
            lightroom_import_base_dir = os.path.expanduser("~/pictures/Lightroom")
            
            all_stems = set(file_db.keys())
            # Treat both input stems (moved_stems) and stacked outputs as "already handled"
            processed_stems = moved_stems | stacked_outputs
            remaining_stems = all_stems - processed_stems

            for stem in sorted(list(remaining_stems)):
                record = file_db[stem]
                for file_type in ['jpg', 'raw']:
                    file_info = record['files'].get(file_type)
                    if not file_info:
                        continue
                    
                    src_path = file_info['path']
                    file_date = get_file_date(file_info, args.verbose)
                    if file_date is None:
                        if args.verbose:
                            print(f"Warning: Could not determine date for '{src_path}', skipping import move.")
                        continue

                    dest_dir_import = os.path.join(
                        lightroom_import_base_dir,
                        str(file_date.year),
                        file_date.strftime('%Y-%m-%d')
                    )

                    ensure_directory_once(dest_dir_import, created_dirs, args.dry_run)
                    
                    dest_path = os.path.join(dest_dir_import, file_info['basename'])

                    if safe_file_operation("move", src_path, dest_path, "moving remaining file", args.force, args.dry_run):
                        remaining_moved_count += 1
                        if args.verbose or args.dry_run:
                            print(f"{ 'Would move' if args.dry_run else 'Moved'} remaining file '{file_info['basename']}' to '{dest_dir_import}'")
                    else:
                        failed_count += 1



            
    else:
        use_parallel_copy = operation_mode in {"copy", "stackcopy"} and args.jobs > 1 and not args.dry_run
        
        if use_parallel_copy:
            with ThreadPoolExecutor(max_workers=args.jobs) as copy_executor:
                pending_copy_jobs = []
                for data in file_db.values():
                    # ... (logic to submit jobs)
                    if data.get('has_jpg') and not data.get('has_raw'):
                        # (The inner logic for submitting jobs remains the same)
                        jpg_record = data['files'].get('jpg')
                        if not jpg_record:
                            continue
                        jpg_path = jpg_record['path']
                        filename = jpg_record['basename']
                        name_stem, ext = os.path.splitext(filename)

                        if is_already_processed(filename):
                            if args.verbose:
                                print(f"Skipping '{filename}' because it already contains 'stacked'.")
                            skipped_count += 1
                            continue

                        if target_date:
                            file_date = get_file_date(jpg_record, args.verbose)
                            if file_date is None or file_date != target_date:
                                continue
                        
                        used_prefix = False

                        if operation_mode == "stackcopy":
                            new_filename = create_new_filename(name_stem, ext, args.prefix)
                            dest_path = os.path.join(dest_dir, new_filename)
                            used_prefix = True
                            future = copy_executor.submit(
                                safe_file_operation, "copy", jpg_path, dest_path, "copying", args.force, args.dry_run
                            )
                            pending_copy_jobs.append({
                                'future': future, 'filename': filename, 'dest_filename': new_filename,
                                'dest_dir': dest_dir, 'used_prefix': used_prefix
                            })
                        elif operation_mode == "copy":
                            if args.prefix:
                                new_filename = create_new_filename(name_stem, ext, args.prefix)
                                dest_path = os.path.join(dest_dir, new_filename)
                                used_prefix = True
                            else:
                                new_filename = filename
                                dest_path = os.path.join(dest_dir, filename)
                            future = copy_executor.submit(
                                safe_file_operation, "copy", jpg_path, dest_path, "copying", args.force, args.dry_run
                            )
                            pending_copy_jobs.append({
                                'future': future, 'filename': filename, 'dest_filename': new_filename,
                                'dest_dir': dest_dir, 'used_prefix': used_prefix
                            })

                for job in pending_copy_jobs:
                    success = False
                    try:
                        success = job['future'].result()
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1
                    except Exception as e: # noqa: BLE001 - top-level CLI error boundary
                        print(f"Error processing '{job['filename']}': {e}")
                        failed_count += 1
                    
                    if args.verbose or args.dry_run:
                        message = format_action_message(
                            operation_mode, job['filename'], job['dest_filename'],
                            job['dest_dir'], success, args.dry_run, job['used_prefix']
                        )
                        print(message)

        else:
            # Sequential processing logic (no ThreadPoolExecutor)
            for data in file_db.values():
                if data.get('has_jpg') and not data.get('has_raw'):
                    jpg_record = data['files'].get('jpg')
                    if not jpg_record:
                        continue
                    jpg_path = jpg_record['path']
                    filename = jpg_record['basename']
                    name_stem, ext = os.path.splitext(filename)

                    if is_already_processed(filename):
                        if args.verbose:
                            print(f"Skipping '{filename}' because it already contains 'stacked'.")
                        skipped_count += 1
                        continue

                    if target_date:
                        file_date = get_file_date(jpg_record, args.verbose)
                        if file_date is None or file_date != target_date:
                            continue

                    used_prefix = False
                    success = None
                    dest_path = ''
                    new_filename = ''

                    if operation_mode == "rename":
                        new_filename = create_new_filename(name_stem, ext, args.prefix)
                        dest_path = os.path.join(dest_dir, new_filename)
                        used_prefix = bool(args.prefix)
                        success = safe_file_operation("move", jpg_path, dest_path, "renaming", args.force, args.dry_run)
                    elif operation_mode == "stackcopy":
                        new_filename = create_new_filename(name_stem, ext, args.prefix)
                        dest_path = os.path.join(dest_dir, new_filename)
                        used_prefix = True
                        success = safe_file_operation("copy", jpg_path, dest_path, "copying", args.force, args.dry_run)
                    elif operation_mode == "copy":
                        if args.prefix:
                            new_filename = create_new_filename(name_stem, ext, args.prefix)
                            dest_path = os.path.join(dest_dir, new_filename)
                            used_prefix = True
                        else:
                            new_filename = filename
                            dest_path = os.path.join(dest_dir, filename)
                        success = safe_file_operation("copy", jpg_path, dest_path, "copying", args.force, args.dry_run)

                    if success is not None:
                        if success:
                            processed_count += 1
                        else:
                            failed_count += 1

                        if args.verbose or args.dry_run:
                            message = format_action_message(
                                operation_mode, filename, os.path.basename(dest_path), 
                                dest_dir, success, args.dry_run, used_prefix
                            )
                            print(message)


    # Print summary
    date_info = f" from {target_date}" if target_date else ""
    prefix_info = f" with prefix '{args.prefix}'" if args.prefix else ""

    if args.dry_run:
        # Custom summary for dry-run
        if operation_mode == "rename":
            print(f"\nDRY RUN: Would rename {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'.")
        elif operation_mode == "lightroom" or operation_mode == "lightroomimport":
            print(
                f"\nDRY RUN: Would process {stack_outputs_seen} stacked JPG files"
                f"{prefix_info} in '{src_dir}' (renaming {processed_count} of them)."
            )
            print(
                f"DRY RUN: Would move {moved_input_count} input files (JPG and ORF) "
                f"to '{LIGHTROOM_BASE_DIR}'."
            )
            if operation_mode == "lightroomimport":
                print(
                    f"DRY RUN: Would move {moved_output_count} stacked output files "
                    f"to '~/pictures/Lightroom'."
                )
                print(
                    f"DRY RUN: Would move {remaining_moved_count} remaining files "
                    f"to '~/pictures/Lightroom'."
                )
        elif operation_mode == "stackcopy":
            print(f"\nDRY RUN: Would copy and rename {processed_count} JPG files{prefix_info} without corresponding raw files to the '{dest_dir}' directory.")
        else: # copy mode
            action_desc = "copy and rename" if args.prefix else "copy"
            print(f"\nDRY RUN: Would {action_desc} {processed_count} JPG files{prefix_info}{date_info} without corresponding raw files to '{dest_dir}'.")
    else:
        # Normal summary
        if operation_mode == "rename":
            print(f"\nDone. Renamed {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'.")
        elif operation_mode == "lightroom" or operation_mode == "lightroomimport":
            print(
                f"\nDone. Processed {stack_outputs_seen} stacked JPG files"
                f"{prefix_info} in '{src_dir}' (renamed {processed_count})."
            )
            print(
                f"Moved {moved_input_count} input files (JPG and ORF) "
                f"to '{LIGHTROOM_BASE_DIR}'."
            )
            if operation_mode == "lightroomimport":
                print(
                    f"Moved {moved_output_count} stacked output files "
                    f"to '~/pictures/Lightroom'."
                )
                print(
                    f"Moved {remaining_moved_count} remaining files "
                    f"to '~/pictures/Lightroom'."
                )
        elif operation_mode == "stackcopy":
            print(f"\nDone. Copied and renamed {processed_count} JPG files{prefix_info} without corresponding raw files to the '{dest_dir}' directory.")
        else: # copy mode
            action_desc = "Copied and renamed" if args.prefix else "Copied"
            print(f"\nDone. {action_desc} {processed_count} JPG files{prefix_info}{date_info} without corresponding raw files to '{dest_dir}'.")

    if skipped_count > 0:
        print(f"Skipped {skipped_count} files that were already processed.")

    if failed_count > 0:
        print(f"Failed to process {failed_count} files.")

if __name__ == "__main__":
    main()
