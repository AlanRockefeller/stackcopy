#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Turn ChangeLog.md into release notes, and refuse to ship a mismatched tag.

Two jobs, one source of truth.  ``ChangeLog.md`` stays the hand-written record;
nothing here rewrites it and no release prose is maintained a second time.

    python packaging/release_notes.py --check --tag v1.6.0
    python packaging/release_notes.py --tag v1.6.0 --output notes.md

``--check`` fails loudly when a tag, ``STACKCOPY_VERSION``, and the changelog
disagree - a tag of ``v1.5.10`` against a source that still says ``1.5.9``
would otherwise produce a build that lies about which version it is.

Build-only tags (``v1.5.9-build2``) are the same application version as
``v1.5.9`` and validate against the same changelog entry, which is exactly why
they must never reach users as a newer version.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stackcopy_updater import (  # noqa: E402
    GITHUB_REPOSITORY,
    normalize_version,
    versions_match,
)

CHANGELOG_PATH = ROOT / "ChangeLog.md"
SOURCE_PATH = ROOT / "stackcopy.py"

_HEADING_RE = re.compile(r"^##(?!#)\s*(.+?)\s*$")
_VERSION_IN_HEADING_RE = re.compile(r"^(\d+(?:\.\d+)*)\b")
# The changelog has accumulated several heading dialects over the years:
#   ## **1.6.0 - 2026-08-24**     ## \*\*1.5.4 - 2026-04-10
#   ## **[1.5.2] - 2026-01-31**   ## Version 1.2 - 2025-11-20
# Strip the decoration rather than trying to match every dialect.
_DECORATION_RE = re.compile(r"^[\s\\*_\[\]#]+|[\s\\*_\[\]#]+$")


class ChangelogError(RuntimeError):
    """The changelog does not support the release being built."""


def heading_version(heading: str) -> str | None:
    """Extract the version a ``##`` heading announces, if it announces one."""
    text = _DECORATION_RE.sub("", heading)
    text = re.sub(r"^(?:version|release)\s+", "", text, flags=re.IGNORECASE)
    text = _DECORATION_RE.sub("", text)
    match = _VERSION_IN_HEADING_RE.match(text)
    if not match:
        return None
    return match.group(1)


def parse_changelog(text: str) -> list[tuple[str, str, str]]:
    """Return ``(version, heading, body)`` for every released section, in order.

    ``## **Unreleased**`` carries no version and is skipped: an unreleased
    section must never become the notes for a tagged release.
    """
    sections: list[tuple[str, str, str]] = []
    current: list[str] | None = None
    version = ""
    heading = ""

    for line in text.splitlines():
        match = _HEADING_RE.match(line)
        if match:
            if current is not None:
                sections.append((version, heading, "\n".join(current).strip()))
                current = None
            found = heading_version(match.group(1))
            if found is None:
                version = ""
                heading = ""
                continue
            version = found
            heading = _DECORATION_RE.sub("", match.group(1))
            current = []
            continue
        if current is not None:
            current.append(line)

    if current is not None:
        sections.append((version, heading, "\n".join(current).strip()))
    return sections


def section_for_version(text: str, version: str) -> tuple[str, str]:
    """Return ``(heading, body)`` for a version, or raise :class:`ChangelogError`."""
    wanted = normalize_version(version)
    if not wanted:
        raise ChangelogError(f"{version!r} is not a version this project can release")

    sections = parse_changelog(text)
    for found_version, heading, body in sections:
        if versions_match(found_version, wanted):
            if not body.strip():
                raise ChangelogError(
                    f"ChangeLog.md has a {wanted} heading but no entries under it."
                )
            return heading, body

    known = ", ".join(version for version, _, _ in sections[:8]) or "none"
    raise ChangelogError(
        f"ChangeLog.md has no section for version {wanted}.\n"
        f"Add a '## **{wanted} - YYYY-MM-DD**' section before tagging.\n"
        f"Most recent changelog versions: {known}"
    )


def source_version(source_text: str) -> str:
    """Read ``STACKCOPY_VERSION`` - the one authoritative runtime version."""
    match = re.search(r'^STACKCOPY_VERSION\s*=\s*"([^"]+)"', source_text, re.MULTILINE)
    if not match:
        raise ChangelogError("Could not find STACKCOPY_VERSION in stackcopy.py")
    return match.group(1)


def validate(tag: str, source_text: str, changelog_text: str) -> str:
    """Check tag, source, and changelog agree. Returns the application version."""
    tag_version = normalize_version(tag)
    if not tag_version:
        raise ChangelogError(
            f"Release tag {tag!r} does not contain a version number. "
            "Tags look like v1.6.0 or v1.6.0-build2."
        )

    declared = source_version(source_text)
    if not versions_match(tag_version, declared):
        raise ChangelogError(
            f"Release tag {tag!r} is version {tag_version}, but stackcopy.py "
            f"declares STACKCOPY_VERSION = {declared!r}.\n"
            "Tag and source must name the same application version. "
            "A build-only tag such as v{0}-build2 is fine; a different "
            "version number is not.".format(declared)
        )

    section_for_version(changelog_text, tag_version)
    return tag_version


def render_notes(tag: str, source_text: str, changelog_text: str) -> str:
    """Build the GitHub release body from the matching changelog section."""
    version = validate(tag, source_text, changelog_text)
    _, body = section_for_version(changelog_text, version)
    tag_note = ""
    if normalize_version(tag) != tag.lstrip("vV").strip():
        # A build re-cut: say so, so nobody reads it as a new version.
        tag_note = (
            f"\nThis is a rebuild of Stackcopy {version} "
            f"(`{tag}`). The application version is unchanged, so Stackcopy "
            "will not offer it as an update.\n"
        )

    changelog_link = f"https://github.com/{GITHUB_REPOSITORY}/blob/main/ChangeLog.md"
    return (
        f"## Stackcopy {version}\n"
        f"{tag_note}\n"
        f"{body}\n\n"
        f"---\n\n"
        f"Full changelog: [ChangeLog.md]({changelog_link})\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Release tag, e.g. v1.6.0")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only validate tag/source/changelog agreement; write nothing.",
    )
    parser.add_argument("--output", help="Write the release notes to this file.")
    parser.add_argument("--changelog", default=str(CHANGELOG_PATH))
    parser.add_argument("--source", default=str(SOURCE_PATH))
    args = parser.parse_args(argv)

    try:
        changelog_text = Path(args.changelog).read_text(encoding="utf-8")
        source_text = Path(args.source).read_text(encoding="utf-8")
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        if args.check:
            version = validate(args.tag, source_text, changelog_text)
            print(
                f"OK: tag {args.tag}, STACKCOPY_VERSION, and ChangeLog.md "
                f"all agree on Stackcopy {version}."
            )
            return 0
        notes = render_notes(args.tag, source_text, changelog_text)
    except ChangelogError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.output:
        Path(args.output).write_text(notes, encoding="utf-8")
        print(f"Wrote release notes for {args.tag} to {args.output}")
    else:
        sys.stdout.write(notes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
