"""Shared test setup.

ExifTool discovery is cached for the life of the process and answers
differently on every developer's machine, so the suite pins it: each test
starts from the version Stackcopy 1.6.0 is tested against, and any test that
cares about a different one patches it explicitly.  No test needs a real
ExifTool installed.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402


@pytest.fixture(autouse=True)
def pinned_exiftool():
    stackcopy._exiftool_info = stackcopy.ExifToolInfo(
        executable="/usr/bin/exiftool",
        version="13.59",
        version_tuple=(13, 59),
        source=stackcopy.ExifToolSource.PATH,
    )
    stackcopy._exiftool_status_reported = False
    yield
    stackcopy.reset_exiftool_info()
    stackcopy._exiftool_status_reported = False
