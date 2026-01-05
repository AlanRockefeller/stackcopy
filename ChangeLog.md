# Change Log


## **[1.4] — 2026-01-04**
- Atomic File Operations (_atomic_copy2 function)

Implements atomic copy by writing to a temporary file first, then using os.replace() to atomically swap it with the destination
Prevents partial/corrupted files if operations are interrupted
Includes cleanup of temp files in case of errors


- New --lightroomimport Mode

Extends --lightroom functionality to also move remaining non-stacked files to ~/pictures/Lightroom/YEAR/DATE/
Moves stacked output JPGs to the Lightroom import directory structure
Tracks three categories: input files (stacked frames), output files (stacked results), and remaining files


- Self-Healing Logic

Detects when destination files exist but are 0 bytes (from interrupted previous runs)
Automatically replaces them with valid source files
Works in both dry-run and normal mode


- New --debug-stacks Flag

Provides detailed diagnostic output showing why stacks are accepted or rejected
Shows timestamp gaps, sequence matching, and safety check results
Very helpful for troubleshooting stack detection issues

## **[1.3] — 2025-11-22**
### **Added**
- Lightroom mode now processes stacked output JPGs *even if already renamed* (i.e., containing `"stacked"` in filename).
- Cross-filesystem safe move handling:
  - Falls back to `copyfile + unlink` instead of `shutil.move()` for SD→disk transfers.
- File deduplication based on content comparison:
  - If destination exists and files are identical:
    - In **move mode**: source is deleted and operation succeeds.
    - In **copy mode**: copy is skipped and treated as success.
- Parallel input-file moves and copies now supported in Lightroom mode when `--jobs` is set - which it is by default.

### **Improved**
- `mtime` is now lazily loaded (`get_file_mtime()`) ensuring accurate timestamp comparison when identifying input frames.
- Lightroom summary now reports:
  - Number of stacked outputs *processed*, not just renamed.
  - Count of RAW/JPG input files moved.
- Reduced repeated warnings when rerunning on partially imported cards.

### **Fixed**
- Previously, already-renamed stacked JPGs were skipped entirely, preventing input frames from being moved.
- `shutil.move()` cross-device failures on WSL/drvfs mount points.

### **Performance**
- Significantly faster import speeds with multi-threaded copy (especially large stacks from SD cards).
- Avoids redundant copies when rerunning Lightroom mode.

## Version 1.2 - 2025-11-20

- Fixed false positive in focus-bracketing burst detection when shooting multiple stacks in quick succession

The --lightroom mode includes a safety check to skip moving input files when it detects a focus-bracketing burst longer than 15 frames. This check was incorrectly comparing the stacked output file's timestamp against raw files from a previous, unrelated stack. When two separate stacks were shot within 20 seconds of each other, the script would mistakenly conclude they were part of one giant burst and skip moving the input files for the second stack.
The fix changes the burst detection to compare timestamps between consecutive raw files rather than between the output and earlier raw files. This correctly identifies actual continuous bursts while allowing separate stacks shot in quick succession to be processed independently.

## Version 1.1 - 2025-10-30

### Added

- `--lightroom` mode: A new mode to streamline the workflow for processing in-camera stacks for use with Adobe Lightroom.  The idea is that you run this on the photos on the camera card before you import to lightroom so you only import the files you need, not all the input files to the stack.
- Identifies in-camera photo stacks (3-15 input files and one output file).
- Moves the input files (both JPG and ORF) of identified stacks to a dated directory structure (e.g., `/home/alan/pictures/olympus.stack.input.photos/2025/2025-10-30/`).
- Renames the stacked output JPG in its original directory.
- Single-shot photos and focus bracketed photos (JPG/ORF pairs not part of a stack) are left untouched.
