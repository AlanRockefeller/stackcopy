# stackcopy
Olympus / OM-System in-camera stacking produces many RAW/JPG frames per final JPG. Lightroom doesn't automatically group or separate them, so imports get cluttered. This script separates originals from stacked outputs automatically, so you only need to manually process the photos that actually need your attention.

## What can it do?
This script finds the stacked images by looking for .jpg files which have no corresponding raw file. This is the only way I have found to detect which images are the stacked versions - the file sizes / exif data is not unique for photos created with in-camera stacking.

The script has five main modes:

- **Copy mode**: Finds JPGs without raw files and copies them somewhere else
- **Rename mode**: Finds those JPGs and renames them in-place by adding " stacked" to the filename
- **Stackcopy mode**: Copies them to a "stacked" subfolder AND adds " stacked" to their names
- **Lightroom mode**: Moves the input files of a stack to a dated folder structure and renames the output file in place. Groups based on numeric sequence and timestamp window - the idea is that in-camera focus stacks are renamed and the inputs saved to a separate place, but single shots or focus bracketing isn't moved since you'll want to process those manually.
- **Lightroom Import mode**: Same as Lightroom mode, but also moves the stacked output JPGs and all remaining files to `~/pictures/Lightroom/YEAR/DATE/` - for a complete import workflow.

Plus, you can filter by date, add custom prefixes, and more.

Now supports:
- Multi-threaded copying/moving (`--jobs`) for better performance
- Atomic file operations - prevents corrupted files from interrupted operations
- Self-healing - automatically recovers from previous interrupted runs
- Cross-device safe move using fallback copy+delete
- Debug mode (`--debug-stacks`) for troubleshooting stack detection

## Installation
Download the script and make it executable:

```bash
wget https://raw.githubusercontent.com/AlanRockefeller/stackcopy/main/stackcopy.py
chmod +x stackcopy.py
```

Or clone the whole repo:

```bash
git clone https://github.com/AlanRockefeller/stackcopy.git
cd stackcopy
```

**Requirements**: Python 3.6 or newer. That's it! No extra packages needed.

## Quick Examples
Here are some real-world scenarios:

### "I want to find all my stacked JPGs and copy them to a folder"
```bash
./stackcopy.py --copy /photos/Lightroom/2025/2025-07-10/ /photos/stacked-images
```

### "Just rename them where they are"
```bash
./stackcopy.py --rename /photos/Lightroom/2025/2025-07-10/
```

### "Put them in a subfolder called 'stacked' with new names"
```bash
./stackcopy.py --stackcopy /photos/Lightroom/2025/2025-07-10/
```

### "Organize my stacks for Lightroom"
This moves the input files (JPG and ORF) of a stack to a dated folder, and renames the output JPG in place.

```bash
./stackcopy.py --lightroom /photos/camera-import/
```

### "Complete Lightroom import workflow"
This does everything: detects stacks, plans all moves, shows a summary, then moves everything oldest-first by photo time. Stack inputs go to a separate directory, stacked outputs and remaining files go to your Lightroom library.

```bash
./stackcopy.py --lightroomimport /photos/camera-import/
```

Want to review the plan before it runs? Use interactive mode:
```bash
./stackcopy.py --lightroomimport /photos/camera-import/ -i
```

### "Show me what would happen without actually doing anything"
Add `--dry` to any command:

```bash
./stackcopy.py --copy /photos/source /photos/dest --dry
```

### "I only want today's photos"
```bash
./stackcopy.py --copy /photos/camera-import /photos/today-stacked --today
```

### "Add a custom prefix to the renamed files"
```bash
./stackcopy.py --stackcopy /photos/mushrooms --prefix "Jackson State Forest"
# Creates files like: "IMG_1234 Jackson State Forest stacked.jpg"
```

### "Debug why stacks aren't being detected correctly"
```bash
./stackcopy.py --lightroom /photos/camera-import/ --debug-stacks --dry
```

## All the Options
Here's what you can do:

### Operation Modes (pick one):
- `--copy SOURCE DEST` - Copy orphaned JPGs from SOURCE to DEST
- `--rename [DIR]` - Rename orphaned JPGs in-place (default: current directory)
- `--stackcopy [DIR]` - Copy to a 'stacked' subfolder with renamed files
- `--lightroom [DIR]` - Move stack input files to a dated folder and rename stack output files in place
- `--lightroomimport [DIR]` - Plans all moves first, then moves files oldest-first by photo time (mtime). Stacked outputs go to `~/pictures/Lightroom/YEAR/DATE/` with a "stacked" suffix, inputs go to a separate dated folder, and remaining files go to `~/pictures/Lightroom/YEAR/DATE/`. Shows a summary before moving.

### Date Filters (for copy operations):
- `--today` - Only process files from today
- `--yesterday` - Only process files from yesterday
- `--date YYYY-MM-DD` - Only process files from a specific date

### Other Options:
- `--prefix PREFIX` - Add a custom prefix before " stacked" in filenames
- `--dry` or `--dry-run` - Preview what would happen without making changes
- `-v` or `--verbose` - Show detailed info about each file processed
- `-i` or `--interactive` - Show a summary and ask for confirmation before moving files (`--lightroomimport` only). Default behavior proceeds automatically.
- `--force` - Overwrite existing files without asking
- `--jobs N` - Use N parallel workers for copying/moving (used by `--lightroom`, `--copy`, `--stackcopy`). `--lightroomimport` always runs sequentially to preserve oldest-first order.
- `--debug-stacks` - Show detailed diagnostic output for stack detection logic

### Data Integrity & Safety
- **Atomic operations**: Files are written to temporary locations first, then atomically moved to prevent corruption from interruptions
- **Self-healing**: Automatically detects and replaces 0-byte placeholder files from interrupted previous runs
- Automatically avoids overwriting different files unless `--force` is set
- If destination file exists and has identical contents, the operation proceeds safely (removing source for moves)
- All file operations are crash-safe - you'll never end up with partial or corrupted files

## How it Works

### Basic Detection
The script finds stacked files by looking for JPG files that don't have corresponding raw files:

1. It scans your folder for all raw files (ORF, CR2, NEF, ARW, DNG, etc.)
2. Then it looks for JPG files that DON'T have a matching raw file
3. It ignores files that already have " stacked" in their name (so you can run it multiple times safely)
4. Then it does whatever operation you asked for

The matching is case-insensitive, so `IMG_1234.JPG` will match with `img_1234.orf` just fine.

### Lightroom Mode Intelligence
In `--lightroom` and `--lightroomimport` modes, the script uses advanced logic to identify which input frames belong to each stacked output:

- **Numeric sequence detection**: Groups files by their numeric stems (e.g., IMG_0100, IMG_0101, IMG_0102)
- **Timestamp analysis**: Confirms frames were taken within 20 seconds of each other
- **Stack size validation**: Accepts stacks with 3-15 input frames
- **Focus bracket protection**: Rejects sequences with more than 15 consecutive frames to avoid moving focus bracketing bursts

Use `--debug-stacks` with `--dry` to see exactly why each stack is accepted or rejected.

## Real-World Examples

### Scenario 1: Import and Organize
You just imported photos from your camera and want to separate the stacked JPGs:

```bash
# First, see what we're dealing with
./stackcopy.py --copy /photos/import /photos/mushrooms --dry

# Looks good? Run it for real
./stackcopy.py --copy /photos/import /photos/mushrooms

# Want more details?
./stackcopy.py --copy /photos/import /photos/mushrooms --verbose
```

### Scenario 2: Complete Lightroom Workflow
Import everything from your camera card, organize stacks automatically, and move to your Lightroom library:

```bash
# Preview what will happen (shows the full plan without moving anything)
./stackcopy.py --lightroomimport /media/camera-card/ --dry --verbose

# Run with interactive confirmation (shows summary, asks before moving)
./stackcopy.py --lightroomimport /media/camera-card/ -i --verbose

# Or just run it (proceeds automatically after showing the summary)
./stackcopy.py --lightroomimport /media/camera-card/ --verbose
```

This will:
1. Scan all files and detect stacked outputs (JPGs without RAWs)
2. Plan all moves and show a summary
3. Move everything oldest-first by photo time:
   - Stack input frames (3-15 RAWs) to `/home/alan/pictures/olympus.stack.input.photos/YEAR/DATE/`
   - Stacked outputs (with " stacked" suffix) to `~/pictures/Lightroom/YEAR/DATE/`
   - All remaining files to `~/pictures/Lightroom/YEAR/DATE/`

### Scenario 3: Troubleshoot Stack Detection
If stacks aren't being detected correctly, use debug mode:

```bash
./stackcopy.py --lightroom /photos/camera-import/ --debug-stacks --dry
```

This shows detailed information about:
- Which files are being considered as stack candidates
- Timestamp gaps between frames
- Why stacks are accepted or rejected
- Whether the burst safety check is triggering

### Scenario 4: Mark HDR Photos
If your camera saves HDR composites as JPG-only, mark them clearly:

```bash
./stackcopy.py --rename /photos/2025/july --prefix "HDR"
```

### Scenario 5: Today's Photo Walk
You went mushroom hunting and want to just copy the stacked photos you took today:

```bash
./stackcopy.py --copy /photos/mushrooms /photos/newstacks --today
```

## Tips & Tricks
- Always run with `--dry` first to see what will happen
- The script won't overwrite files unless you use `--force`
- Files already containing " stacked" are automatically skipped
- You can run the script multiple times safely - it won't double-process files
- The date filters use file modification time (most reliable across platforms)
- Use `--jobs 4` or higher for faster processing in `--lightroom`, `--copy`, or `--stackcopy` modes
- If operations are interrupted, just re-run - the self-healing logic will fix any incomplete files
- Use `--debug-stacks` with `--dry` to understand why certain photo sequences aren't being treated as stacks
- In Lightroom Import mode, all files end up in `~/pictures/Lightroom/YEAR/DATE/` organized by date

## Version Info
- **Version**: 1.5.3
- **Date**: March 26, 2026
- **Author**: Alan Rockefeller
- **Repository**: https://github.com/AlanRockefeller/stackcopy
- **License**: MIT

## License
MIT License - basically, do whatever you want with it! See the LICENSE file for the legal details.

---
