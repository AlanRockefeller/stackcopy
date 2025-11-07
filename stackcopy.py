#!/usr/bin/python3
# SPDX-License-Identifier: MIT

# Stackcopy version 1.1 by Alan Rockefeller
# October 30, 2025

# Copies / renames only the photos that have been stacked in-camera - designed for Olympus / OM System, though it might work for other cameras too.

import sys
import os
import shutil
import argparse
import re
from datetime import datetime, date, timedelta

LIGHTROOM_BASE_DIR = "/home/alan/pictures/olympus.stack.input.photos/"


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

def is_already_processed(filename):
    """Check if a file has already been processed (contains 'stacked' as a word)."""
    stem, ext = os.path.splitext(filename)
    # Use word boundary regex to match 'stacked' as a complete word
    # This will match: "image stacked.jpg", "stacked_image.jpg", "stacked-photo.jpg", etc.
    return bool(re.search(r'\bstacked\b', stem.lower()))

def get_file_date(file_path):
    """Get the modification date of a file (most reliable across platforms)."""
    try:
        # Use modification time as it's more reliable across platforms
        # Note: Creation time is not reliably available on all systems
        timestamp = os.path.getmtime(file_path)
        return datetime.fromtimestamp(timestamp).date()
    except OSError:
        return None

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

def safe_file_operation(operation, src_path, dest_path, operation_name, force=False, dry_run=False):
    """Safely perform file operations with error handling and overwrite protection."""
    # Check if destination exists (even in dry-run mode for accurate preview)
    if os.path.exists(dest_path) and not force:
        if dry_run:
            print(f"Warning: '{dest_path}' already exists. Would need --force to overwrite.")
            return False
        else:
            print(f"Warning: '{dest_path}' already exists. Use --force to overwrite.")
            return False
    
    # If dry run, we've done our checks, now return success
    if dry_run:
        return True

    try:
        if operation == "move":
            shutil.move(src_path, dest_path)
        elif operation == "copy":
            shutil.copy2(src_path, dest_path)
        return True
    except (OSError, shutil.Error) as e:
        print(f"Error {operation_name} '{src_path}' to '{dest_path}': {e}")
        return False

def format_action_message(operation_mode, filename, dest_filename, dest_dir, success, dry_run, used_prefix):
    """Generate consistent action messages for all operations."""
    if operation_mode == "rename" or operation_mode == "lightroom":
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

    # Add date filtering options
    date_group = parser.add_argument_group('Date Filtering (optional, for copy operations)')
    date_group.add_argument("--today", action="store_true", help="Process JPGs created today that don't have a corresponding raw file.")
    date_group.add_argument("--yesterday", action="store_true", help="Process JPGs created yesterday that don't have a corresponding raw file.")
    date_group.add_argument("--date", metavar='YYYY-MM-DD', help="Process JPGs from a specific date that don't have a corresponding raw file.")

    # Add prefix option
    parser.add_argument("--prefix", metavar='PREFIX',
                        help="Add a custom prefix before ' stacked' in the filename when using --copy, --rename, or --stackcopy.")

    # Add dry-run option
    parser.add_argument("--dry", "--dry-run", action="store_true",
                        help="Show what would happen without making any actual changes")

    # Add verbose flag
    parser.add_argument("-v", "--verbose", action="store_true", help="Show detailed information about processed files")

    # Add overwrite protection option
    parser.add_argument("--force", action="store_true", help="Overwrite existing files without prompting")

    # Parse arguments
    args = parser.parse_args()

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
    if not args.copy and args.rename is None and args.stackcopy is None and args.lightroom is None:
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
        if not args.dry:
            try:
                os.makedirs(dest_dir, exist_ok=True)
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
    elif args.lightroom is not None: # --lightroom mode
        operation_mode = "lightroom"
        work_dir = normalize_path(args.lightroom)

        # Verify that the specified directory exists
        if not os.path.isdir(work_dir):
            print(f"Error: Directory '{work_dir}' does not exist or is not a directory.")
            sys.exit(1)

        # For lightroom mode, source and working directory are the same
        src_dir = work_dir
        dest_dir = work_dir  # We're renaming in-place

        # Ensure the Lightroom base directory exists
        if not args.dry:
            try:
                os.makedirs(LIGHTROOM_BASE_DIR, exist_ok=True)
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
        if not args.dry:
            try:
                os.makedirs(dest_dir, exist_ok=True)
            except OSError as e:
                print(f"Error creating stacked directory '{dest_dir}': {e}")
                sys.exit(1)


    # Define a list of common raw photo extensions
    RAW_EXTENSIONS = {'.orf', '.cr2', '.nef', '.arw', '.dng', '.pef', '.rw2', '.raf', '.raw', '.sr2'}
    JPG_EXTENSIONS = {'.jpg', '.jpeg'}

    # --- 1. Scan directory and build file database ---
    file_db = {}
    try:
        for entry in os.scandir(src_dir):
            if entry.is_file():
                stem, ext = os.path.splitext(entry.name)
                ext_lower = ext.lower()

                if stem not in file_db:
                    file_db[stem] = {'paths': {}}
                
                if ext_lower in RAW_EXTENSIONS:
                    file_db[stem]['has_raw'] = True
                    file_db[stem]['paths']['raw'] = entry.path
                elif ext_lower in JPG_EXTENSIONS:
                    file_db[stem]['has_jpg'] = True
                    file_db[stem]['paths']['jpg'] = entry.path

    except OSError as e:
        print(f"Error scanning source directory '{src_dir}': {e}")
        sys.exit(1)


    # --- 2. Process files based on operation mode ---
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    moved_input_count = 0

    if args.lightroom is not None:
        # --- Lightroom Mode ---
        # Find stacked output files (JPG without ORF)
        stacked_outputs = []
        for stem, data in file_db.items():
            if data.get('has_jpg') and not data.get('has_raw'):
                jpg_path = data['paths']['jpg']
                if not is_already_processed(jpg_path):
                    if target_date:
                        file_date = get_file_date(jpg_path)
                        if file_date is None or file_date != target_date:
                            continue
                    stacked_outputs.append(stem)
        
        # Sort to process sequentially
        stacked_outputs.sort()

        moved_stems = set()

        for output_stem in stacked_outputs:
            # Rename the stacked output file
            jpg_path = file_db[output_stem]['paths']['jpg']
            stem, ext = os.path.splitext(os.path.basename(jpg_path))
            new_filename = create_new_filename(stem, ext, args.prefix)
            dest_path = os.path.join(dest_dir, new_filename)
            
            if safe_file_operation("move", jpg_path, dest_path, "renaming", args.force, args.dry):
                processed_count += 1
                if args.verbose or args.dry:
                    print(format_action_message(operation_mode, os.path.basename(jpg_path), new_filename, dest_dir, True, args.dry, bool(args.prefix)))
            else:
                failed_count += 1

            # Find associated input files (3-15 sequential pairs before the output)
            match = re.match(r'([a-zA-Z0-9_-]*)(\d{6,})', output_stem)
            if not match:
                continue

            prefix, num_str = match.groups()
            num = int(num_str)
            
            potential_inputs = []
            for i in range(num - 1, num - 16, -1):
                input_stem = f"{prefix}{str(i).zfill(len(num_str))}"
                if input_stem in file_db and file_db[input_stem].get('has_raw') and input_stem not in moved_stems:
                    potential_inputs.append(input_stem)
                else:
                    break # Sequence broken
            
            if 3 <= len(potential_inputs) <= 15:
                # Valid stack found, move the input files
                for input_stem in potential_inputs:
                    file_date = get_file_date(file_db[input_stem]['paths']['raw'])
                    if file_date is None:
                        if args.verbose:
                            print(f"Warning: Could not determine date for '{input_stem}', skipping move.")
                        continue

                    lightroom_dest_dir = os.path.join(LIGHTROOM_BASE_DIR, str(file_date.year), file_date.strftime('%Y-%m-%d'))
                    
                    for file_type in ['jpg', 'raw']:
                        if file_type in file_db[input_stem]['paths']:
                            src_path = file_db[input_stem]['paths'][file_type]
                            dest_path = os.path.join(lightroom_dest_dir, os.path.basename(src_path))

                            if not args.dry:
                                os.makedirs(lightroom_dest_dir, exist_ok=True)

                            if safe_file_operation("move", src_path, dest_path, "moving input file", args.force, args.dry):
                                moved_input_count += 1
                                if args.verbose or args.dry:
                                    print(f"{'Would move' if args.dry else 'Moved'} input file '{os.path.basename(src_path)}' to '{lightroom_dest_dir}'")
                            else:
                                failed_count += 1
                    moved_stems.add(input_stem)

    else:
        # --- Other Modes (copy, rename, stackcopy) ---
        for stem, data in file_db.items():
            if data.get('has_jpg') and not data.get('has_raw'):
                jpg_path = data['paths']['jpg']
                filename = os.path.basename(jpg_path)
                stem, ext = os.path.splitext(filename)

                if is_already_processed(filename):
                    if args.verbose:
                        print(f"Skipping '{filename}' because it already contains 'stacked'.")
                    skipped_count += 1
                    continue

                if target_date:
                    file_date = get_file_date(jpg_path)
                    if file_date is None or file_date != target_date:
                        continue

                used_prefix = False
                success = False
                if operation_mode == "rename":
                    new_filename = create_new_filename(stem, ext, args.prefix)
                    dest_path = os.path.join(dest_dir, new_filename)
                    used_prefix = bool(args.prefix)
                    success = safe_file_operation("move", jpg_path, dest_path, "renaming", args.force, args.dry)
                elif operation_mode == "stackcopy":
                    new_filename = create_new_filename(stem, ext, args.prefix)
                    dest_path = os.path.join(dest_dir, new_filename)
                    used_prefix = True
                    success = safe_file_operation("copy", jpg_path, dest_path, "copying", args.force, args.dry)
                elif operation_mode == "copy":
                    if args.prefix:
                        new_filename = create_new_filename(stem, ext, args.prefix)
                        dest_path = os.path.join(dest_dir, new_filename)
                        used_prefix = True
                    else:
                        new_filename = filename
                        dest_path = os.path.join(dest_dir, filename)
                    success = safe_file_operation("copy", jpg_path, dest_path, "copying", args.force, args.dry)

                if success:
                    processed_count += 1
                else:
                    failed_count += 1

                if args.verbose or args.dry:
                    message = format_action_message(
                        operation_mode, filename, os.path.basename(dest_path), 
                        dest_dir, success, args.dry, used_prefix
                    )
                    print(message)

    # Print summary
    date_info = f" from {target_date}" if target_date else ""
    prefix_info = f" with prefix '{args.prefix}'" if args.prefix else ""

    if args.dry:
        # Custom summary for dry-run
        if operation_mode == "rename":
            print(f"\nDRY RUN: Would rename {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'.")
        elif operation_mode == "lightroom":
            print(f"\nDRY RUN: Would rename {processed_count} stacked JPG files{prefix_info} in '{src_dir}'.")
            print(f"DRY RUN: Would move {moved_input_count} input files (JPG and ORF) to '{LIGHTROOM_BASE_DIR}'.")
        elif operation_mode == "stackcopy":
            print(f"\nDRY RUN: Would copy and rename {processed_count} JPG files{prefix_info} without corresponding raw files to the '{dest_dir}' directory.")
        else: # copy mode
            action_desc = "copy and rename" if args.prefix else "copy"
            print(f"\nDRY RUN: Would {action_desc} {processed_count} JPG files{prefix_info}{date_info} without corresponding raw files to '{dest_dir}'.")
    else:
        # Normal summary
        if operation_mode == "rename":
            print(f"\nDone. Renamed {processed_count} JPG files{prefix_info} without corresponding raw files in '{dest_dir}'.")
        elif operation_mode == "lightroom":
            print(f"\nDone. Renamed {processed_count} stacked JPG files{prefix_info} in '{src_dir}'.")
            print(f"Moved {moved_input_count} input files (JPG and ORF) to '{LIGHTROOM_BASE_DIR}'.")
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
