# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for the Stackcopy GUI.
#
# Build (must run ON the target OS — PyInstaller cannot cross-compile):
#   pip install -r requirements-gui.txt -r requirements-build.txt
#   pyinstaller packaging/stackcopy_gui.spec
#
# Produces:
#   macOS            -> dist/Stackcopy.app
#   Windows / Linux  -> dist/Stackcopy(.exe)   (single file)
#
# Drop packaging/stackcopy.icns (mac) or packaging/stackcopy.ico (win) to add an
# icon; the spec picks it up automatically if present.

import os
import sys

from PyInstaller.utils.hooks import collect_data_files

# Paths are anchored to this spec's location so the build works from any cwd.
ROOT = os.path.abspath(os.path.join(SPECPATH, os.pardir))

is_mac = sys.platform == "darwin"
icon_path = os.path.join(SPECPATH, "stackcopy.icns" if is_mac else "stackcopy.ico")
icon = icon_path if os.path.exists(icon_path) else None

# customtkinter ships theme/asset files that must be bundled alongside the code.
datas = collect_data_files("customtkinter")

a = Analysis(
    [os.path.join(ROOT, "stackcopy_gui.py")],
    pathex=[ROOT],
    binaries=[],
    datas=datas,
    # stackcopy is imported lazily/conditionally; name it explicitly so the
    # whole CLI is bundled and the frozen app can relaunch itself as the CLI.
    hiddenimports=["stackcopy"],
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
            "CFBundleShortVersionString": "1.0.0",
        },
    )
else:
    # Single self-contained executable for Windows (and Linux).
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
