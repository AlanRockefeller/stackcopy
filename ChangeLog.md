# Change Log

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
