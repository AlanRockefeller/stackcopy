# Change Log

## Version 1.1 - 2025-10-30

### Added

- `--lightroom` mode: A new mode to streamline the workflow for processing in-camera stacks for use with Adobe Lightroom.  The idea is that you run this on the photos on the camera card before you import to lightroom so you only import the files you need, not all the input files to the stack.
  - Identifies in-camera photo stacks (3-15 input files and one output file).
  - Moves the input files (both JPG and ORF) of identified stacks to a dated directory structure (e.g., `/home/alan/pictures/olympus.stack.input.photos/2025/2025-10-30/`).
  - Renames the stacked output JPG in its original directory.
  - Single-shot photos and focus bracketed photos (JPG/ORF pairs not part of a stack) are left untouched.
