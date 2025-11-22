# stackcopy

Olympus / OM-System in-camera stacking produces many RAW/JPG frames per final JPG. Lightroom doesn’t automatically group or separate them, so imports get cluttered. This script separates originals from stacked outputs automatically, so you only need to manually process the photos that actually need your attention.


## What can it do?

This script finds the stacked images by looking for .jpg files which have no corresponding raw file.   This is the only way I have found to detect which images are the stacked versions - the file sizes / exif data is not unique for photos created with in-camera stacking.

The script has four main modes:

- **Copy mode**: Finds JPGs without raw files and copies them somewhere else
- **Rename mode**: Finds those JPGs and renames them in-place by adding " stacked" to the filename
- **Stackcopy mode**: Copies them to a "stacked" subfolder AND adds " stacked" to their names
- **Lightroom mode**: Moves the input files of a stack to a dated folder structure and renames the output file in place.  Groups based on numeric sequence and timestamp window - the idea is that in-camera focus stacks are renamed and the inputs saved to a separate place, but single shots or focus bracketing isn't moved since you'll want to process those manually.    

Plus, you can filter by date, add custom prefixes, and more. 

Now supports:

- Multi-threaded copying (--jobs) for better performance

- Cross-device safe move using fallback copy+delete


## Installation

Just grab the script and make it executable:

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

### "Just rename them where they are so I know which ones to keep"

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

## All the Options

Here's what you can do:

### Operation Modes (pick one):
- `--copy SOURCE DEST` - Copy orphaned JPGs from SOURCE to DEST
- `--rename [DIR]` - Rename orphaned JPGs in-place (default: current directory)
- `--stackcopy [DIR]` - Copy to a 'stacked' subfolder with renamed files
- `--lightroom [DIR]` - Move stack input files to a dated folder and rename stack output files in place.

### Date Filters (for copy operations):
- `--today` - Only process files from today
- `--yesterday` - Only process files from yesterday  
- `--date YYYY-MM-DD` - Only process files from a specific date

### Other Options:
- `--prefix PREFIX` - Add a custom prefix before " stacked" in filenames
- `--dry` or `--dry-run` - Preview what would happen without making changes
- `-v` or `--verbose` - Show detailed info about each file processed
- `--force` - Overwrite existing files without asking
- '--jobs' N              Use multiple threads for copying (faster on large imports)

### Data Integrity & Safety

- Automatically avoids overwriting different files unless `--force` is set
- If destination file exists and has identical file contents, the move proceeds safely


## How it Works

The script find stacked files by looking for jpg files that don't have corresponding raw files

1. It scans your folder for all raw files (ORF, CR2, NEF, ARW, DNG, etc.)
2. Then it looks for JPG files that DON'T have a matching raw file
3. It ignores files that already have " stacked" in their name (so you can run it multiple times safely)
4. Then it does whatever operation you asked for

The matching is case-insensitive, so `IMG_1234.JPG` will match with `img_1234.orf` just fine.

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

### Scenario 2: Mark HDR Photos
If your camera saves HDR composites as JPG-only. Mark them clearly:

```bash
./stackcopy.py --rename /photos/2025/july --prefix "HDR"
```

### Scenario 3: Today's Photo Walk
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

## Version Info

- **Version**: 1.3
- **Date**: November 22, 2025
- **Author**: Alan Rockefeller
- **Repository**: https://github.com/AlanRockefeller/stackcopy
- **License**: MIT

## License

MIT License - basically, do whatever you want with it! See the LICENSE file for the legal details.

---

*
