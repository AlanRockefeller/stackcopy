# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for the Stackcopy GUI.
#
# Build (must run ON the target OS — PyInstaller cannot cross-compile):
#   pip install -r requirements-gui.txt -r requirements-build.txt
#   pyinstaller packaging/stackcopy_gui.spec
#
# Produces:
#   macOS            -> dist/Stackcopy.app
#   Windows          -> dist/Stackcopy/Stackcopy.exe + StackcopyCLI.exe
#   Linux            -> dist/Stackcopy   (single file)
#
# Drop packaging/stackcopy.icns (mac) or packaging/stackcopy.ico (win) to add an
# icon; the spec picks it up automatically if present.
#
# Windows builds also bundle ExifTool so GUI users get OM-1 camera stack
# metadata without installing a hidden dependency.  Run
#   python packaging/fetch_exiftool.py
# first; it verifies a pinned SHA-256 and fails rather than bundling anything
# unexpected.  If packaging/vendor/exiftool is absent the build still succeeds
# and the app falls back to ExifTool on PATH, reporting which one it found.

import os
import re
import sys

from PyInstaller.utils.hooks import collect_data_files

# Paths are anchored to this spec's location so the build works from any cwd.
ROOT = os.path.abspath(os.path.join(SPECPATH, os.pardir))
# So canonical_version() can share the one build-suffix rule with the updater
# instead of keeping a second copy of it here.
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

is_mac = sys.platform == "darwin"
is_windows = sys.platform.startswith("win")
icon_path = os.path.join(SPECPATH, "stackcopy.icns" if is_mac else "stackcopy.ico")
icon = icon_path if os.path.exists(icon_path) else None

# customtkinter ships theme/asset files that must be bundled alongside the code.
datas = collect_data_files("customtkinter")

# The GUI's "View Changelog" button prefers a local copy over the GitHub page,
# so the packaged app carries one.
changelog_path = os.path.join(ROOT, "ChangeLog.md")
if os.path.isfile(changelog_path):
    datas.append((changelog_path, "."))

# The official Windows ExifTool package is exiftool.exe plus an exiftool_files
# support directory that has to stay beside it, so the whole tree is copied in
# under "exiftool/" - which is where stackcopy._bundled_exiftool_path() looks.
exiftool_dir = os.path.join(SPECPATH, "vendor", "exiftool")
bundled_exiftool = False
if is_windows and os.path.isdir(exiftool_dir):
    for current_root, _dirs, files in os.walk(exiftool_dir):
        relative = os.path.relpath(current_root, exiftool_dir)
        target = "exiftool" if relative == os.curdir else os.path.join("exiftool", relative)
        for name in files:
            datas.append((os.path.join(current_root, name), target))
    bundled_exiftool = True
print(
    "Bundling ExifTool: yes"
    if bundled_exiftool
    else "Bundling ExifTool: no (using ExifTool from PATH at runtime)"
)
def canonical_version():
    """Read STACKCOPY_VERSION out of stackcopy.py - the one source of truth.

    The app bundle's version must be the same number the CLI prints and the
    GUI shows.  When the workflow passes a tag through STACKCOPY_VERSION it is
    treated as a cross-check rather than a second source: a tag that disagrees
    with the code is a mistake worth failing the build over, not something to
    silently stamp into the bundle.
    """
    source = open(os.path.join(ROOT, "stackcopy.py"), encoding="utf-8").read()
    match = re.search(r'^STACKCOPY_VERSION\s*=\s*"([^"]+)"', source, re.MULTILINE)
    if not match:
        raise SystemExit("Could not find STACKCOPY_VERSION in stackcopy.py")
    version = match.group(1)

    from stackcopy_updater import normalize_version

    raw = (os.environ.get("STACKCOPY_VERSION") or "").strip()
    # A build-only tag re-cuts the same application version, so v1.6.0-build2
    # agrees with a source that says 1.6.0.  A different version number does
    # not, and must not be stamped into the bundle.
    requested = normalize_version(raw)
    # "0.0.0" is the workflow's placeholder for an untagged build.
    if requested and requested != "0.0.0" and requested != normalize_version(version):
        raise SystemExit(
            f"STACKCOPY_VERSION={raw!r} is version {requested!r}, which does "
            f"not match stackcopy.py ({version!r}). Tag and source must agree."
        )
    return version


bundle_version = canonical_version()
print(f"Stackcopy version: {bundle_version}")

gui_entry = os.path.join(ROOT, "stackcopy_gui.py")
cli_entry = os.path.join(ROOT, "stackcopy_cli.py")

a = Analysis(
    [gui_entry],
    pathex=[ROOT],
    binaries=[],
    datas=datas,
    # stackcopy is imported lazily/conditionally; name it explicitly so the
    # GUI bundle still contains the fallback CLI dispatcher.
    hiddenimports=["stackcopy", "stackcopy_updater"],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

if is_mac:
    # One-folder build wrapped into a .app bundle.
    exe = EXE(
        pyz,
        a.scripts,
        [],
        exclude_binaries=True,
        name="Stackcopy",
        debug=False,
        strip=False,
        upx=False,
        console=False,
        icon=icon,
    )
    coll = COLLECT(
        exe,
        a.binaries,
        a.datas,
        strip=False,
        upx=False,
        name="Stackcopy",
    )
    app = BUNDLE(
        coll,
        name="Stackcopy.app",
        icon=icon,
        bundle_identifier="com.alanrockefeller.stackcopy",
        info_plist={
            "NSHighResolutionCapable": True,
            "CFBundleShortVersionString": bundle_version,
        },
    )
elif is_windows:
    cli_a = Analysis(
        [cli_entry],
        pathex=[ROOT],
        binaries=[],
        datas=[],
        hiddenimports=[],
        hookspath=[],
        runtime_hooks=[],
        excludes=[],
        noarchive=False,
    )
    cli_pyz = PYZ(cli_a.pure)

    # One-folder Windows bundle. The GUI executable stays windowed while the
    # CLI helper uses PyInstaller's console bootloader so stdout/stderr can be
    # redirected into the GUI's live log.
    exe = EXE(
        pyz,
        a.scripts,
        a.dependencies,
        [],
        exclude_binaries=True,
        name="Stackcopy",
        debug=False,
        strip=False,
        upx=False,
        console=False,
        icon=icon,
    )
    cli_exe = EXE(
        cli_pyz,
        cli_a.scripts,
        cli_a.dependencies,
        [],
        exclude_binaries=True,
        name="StackcopyCLI",
        debug=False,
        strip=False,
        upx=False,
        console=True,
        icon=icon,
    )
    coll = COLLECT(
        exe,
        cli_exe,
        a.binaries,
        a.datas,
        cli_a.binaries,
        cli_a.datas,
        strip=False,
        upx=False,
        name="Stackcopy",
    )
else:
    # Single self-contained executable for Linux.
    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.datas,
        [],
        name="Stackcopy",
        debug=False,
        strip=False,
        upx=False,
        runtime_tmpdir=None,
        console=False,
        icon=icon,
    )
