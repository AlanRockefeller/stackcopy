#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Fetch the pinned ExifTool build that the packaged Windows GUI ships with.

Ordinary GUI users should not have to discover and install a hidden
dependency before Stackcopy can read an OM-1's camera-declared stack
metadata, so the Windows package bundles ExifTool.  It is fetched here, at
build time, from a pinned release with a pinned SHA-256: nothing is ever
downloaded at runtime, and a checksum that does not match fails the build
rather than producing an app containing an unknown executable.

Windows only.  The official Windows package is a self-contained executable
plus its ``exiftool_files`` support directory, which drops straight into a
PyInstaller bundle.  macOS ships ExifTool as a .pkg installer or as a Perl
distribution that would depend on the deprecated system Perl, so macOS builds
deliberately bundle nothing and the app tells the user how to install it (see
build/INSTALL-macOS.md).

Usage:
    python packaging/fetch_exiftool.py [--archive PATH] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

# The version Stackcopy 1.6.0 is tested against.  Bumping this means bumping
# the checksum in the same commit; there is no "latest" lookup on purpose.
EXIFTOOL_VERSION = "13.59"
ARCHIVE_NAME = f"exiftool-{EXIFTOOL_VERSION}_64.zip"
ARCHIVE_URL = f"https://sourceforge.net/projects/exiftool/files/{ARCHIVE_NAME}/download"
ARCHIVE_SHA256 = "44b512b25af500724ba579d0a53c8fc5851628b692dd5e5d94ae4a15c2cba9ec"

# Inside the zip: "exiftool(-k).exe" opens its own documentation when
# double-clicked; ExifTool's own README says to rename it for command-line
# use, which is the only way Stackcopy ever calls it.
ARCHIVE_ROOT = f"exiftool-{EXIFTOOL_VERSION}_64"
SOURCE_EXECUTABLE = "exiftool(-k).exe"
SUPPORT_DIRECTORY = "exiftool_files"

PACKAGING_DIR = Path(__file__).resolve().parent
VENDOR_DIR = PACKAGING_DIR / "vendor" / "exiftool"


class ChecksumError(RuntimeError):
    """The archive is not the release this build was pinned to."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download(destination: Path) -> None:
    print(f"Downloading {ARCHIVE_URL}")
    request = urllib.request.Request(
        ARCHIVE_URL, headers={"User-Agent": "stackcopy-build"}
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        with open(destination, "wb") as handle:
            shutil.copyfileobj(response, handle)


def verify(archive: Path) -> None:
    actual = sha256_of(archive)
    if actual != ARCHIVE_SHA256:
        raise ChecksumError(
            f"{archive.name} has SHA-256 {actual}, expected {ARCHIVE_SHA256}. "
            "Refusing to bundle an executable that is not the pinned release."
        )
    print(f"Verified SHA-256 {actual}")


def extract(archive: Path, target: Path) -> None:
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    with tempfile.TemporaryDirectory() as staging_name:
        staging = Path(staging_name)
        with zipfile.ZipFile(archive) as bundle:
            for entry in bundle.namelist():
                # Refuse anything that would write outside the staging tree.
                resolved = (staging / entry).resolve()
                if not str(resolved).startswith(str(staging.resolve())):
                    raise RuntimeError(f"unsafe archive entry: {entry}")
            bundle.extractall(staging)
        root = staging / ARCHIVE_ROOT
        executable = root / SOURCE_EXECUTABLE
        support = root / SUPPORT_DIRECTORY
        if not executable.is_file() or not support.is_dir():
            raise RuntimeError(
                f"{archive.name} does not have the expected " f"{ARCHIVE_ROOT}/ layout"
            )
        shutil.copy2(executable, target / "exiftool.exe")
        shutil.copytree(support, target / SUPPORT_DIRECTORY)
        # ExifTool is redistributed under the same terms as Perl (GPL or the
        # Artistic License); its licence and readme travel with the binary.
        for extra in ("README.txt",):
            source = root / extra
            if source.is_file():
                shutil.copy2(source, target / extra)
    print(f"Bundled ExifTool {EXIFTOOL_VERSION} into {target}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive",
        help="use an already-downloaded copy instead of fetching it",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-extract even if the vendor directory already looks complete",
    )
    parser.add_argument(
        "--any-platform",
        action="store_true",
        help="prepare the Windows payload even when not building on Windows",
    )
    args = parser.parse_args()

    if not sys.platform.startswith("win") and not args.any_platform:
        print(
            "Not a Windows build - ExifTool is not bundled here. "
            "Install it from https://exiftool.org/ (or `brew install exiftool`)."
        )
        return 0

    if (VENDOR_DIR / "exiftool.exe").is_file() and not args.force:
        print(f"ExifTool already present in {VENDOR_DIR}")
        return 0

    with tempfile.TemporaryDirectory() as work_name:
        archive = Path(args.archive) if args.archive else Path(work_name) / ARCHIVE_NAME
        if not args.archive:
            download(archive)
        verify(archive)
        extract(archive, VENDOR_DIR)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ChecksumError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(2)
    except Exception as error:  # noqa: BLE001 - any failure must fail the build
        print(f"ERROR: could not prepare bundled ExifTool: {error}", file=sys.stderr)
        sys.exit(1)
