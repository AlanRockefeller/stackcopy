"""Release notes from ChangeLog.md, and the tag/source/changelog agreement gate.

A tag of v1.5.10 against a source that still says 1.5.9 would build apps that
report a version nobody wrote notes for, and would then tell every 1.5.9 user
to "update" to a build that is not the version it claims.  These tests hold the
gate that stops it.
"""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "packaging"))

import release_notes  # noqa: E402
import stackcopy  # noqa: E402
import stackcopy_updater as updater  # noqa: E402

CHANGELOG = (ROOT / "ChangeLog.md").read_text(encoding="utf-8")
SOURCE = (ROOT / "stackcopy.py").read_text(encoding="utf-8")

SAMPLE_CHANGELOG = """# Change Log

## **Unreleased**

### Added

- Something not released yet.

## **1.6.0 - 2026-08-24**

### Added

- Metadata-confirmed stacks.

### Fixed

- A crash.

## **1.5.9 - 2026-08-24**

### Fixed

- Date filtering.

## \\*\\*1.5.4 - 2026-04-10

- An older heading dialect.

## **[1.5.2] - 2026-01-31**

- A bracketed heading.

## Version 1.2 - 2025-11-20

- The oldest heading dialect.
"""

SAMPLE_SOURCE = 'STACKCOPY_VERSION = "1.6.0"\n'


class ChangelogParsingTests(unittest.TestCase):
    def test_every_heading_dialect_in_the_real_changelog_parses(self):
        sections = release_notes.parse_changelog(CHANGELOG)
        versions = [version for version, _, _ in sections]
        self.assertIn("1.6.0", versions)
        self.assertIn("1.5.9", versions)
        self.assertIn("1.5.4", versions)  # ## \*\*1.5.4 - ...
        self.assertIn("1.5.2", versions)  # ## **[1.5.2] — ...
        self.assertIn("1.2", versions)  # ## Version 1.2 - ...
        self.assertNotIn("", versions)

    def test_every_parsed_section_has_a_body(self):
        for version, _, body in release_notes.parse_changelog(CHANGELOG):
            with self.subTest(version=version):
                self.assertTrue(body.strip(), f"{version} parsed with an empty body")

    def test_the_unreleased_section_is_never_a_release(self):
        versions = [v for v, _, _ in release_notes.parse_changelog(SAMPLE_CHANGELOG)]
        self.assertEqual(versions, ["1.6.0", "1.5.9", "1.5.4", "1.5.2", "1.2"])

    def test_headings_are_read_out_of_their_decoration(self):
        for heading, expected in (
            ("**1.6.0 - 2026-08-24**", "1.6.0"),
            ("\\*\\*1.5.4 - 2026-04-10", "1.5.4"),
            ("**[1.5.2] — 2026-01-31**", "1.5.2"),
            ("Version 1.2 - 2025-11-20", "1.2"),
            ("Release 2.0", "2.0"),
            ("[1.5] — 2026-01-19", "1.5"),
            ("**Unreleased**", None),
            ("Change Log", None),
            ("Notes", None),
        ):
            with self.subTest(heading=heading):
                self.assertEqual(release_notes.heading_version(heading), expected)

    def test_a_section_body_stops_at_the_next_version(self):
        _, body = release_notes.section_for_version(SAMPLE_CHANGELOG, "1.5.9")
        self.assertIn("Date filtering", body)
        self.assertNotIn("Metadata-confirmed", body)
        self.assertNotIn("older heading dialect", body)

    def test_a_missing_version_fails_loudly_rather_than_publishing_nothing(self):
        with self.assertRaises(release_notes.ChangelogError) as caught:
            release_notes.section_for_version(SAMPLE_CHANGELOG, "9.9.9")
        message = str(caught.exception)
        self.assertIn("no section for version 9.9.9", message)
        self.assertIn("before tagging", message)

    def test_a_heading_with_no_entries_under_it_is_refused(self):
        empty = "# Change Log\n\n## **1.7.0 - 2026-09-01**\n\n## **1.6.0 - 2026-08-24**\n\n- Real.\n"
        with self.assertRaises(release_notes.ChangelogError) as caught:
            release_notes.section_for_version(empty, "1.7.0")
        self.assertIn("no entries", str(caught.exception))

    def test_a_two_component_version_matches_its_padded_tag(self):
        heading, _ = release_notes.section_for_version(SAMPLE_CHANGELOG, "1.2.0")
        self.assertIn("1.2", heading)


class VersionAgreementTests(unittest.TestCase):
    def test_the_real_repository_is_self_consistent_right_now(self):
        version = stackcopy.STACKCOPY_VERSION
        release_notes.validate(f"v{version}", SOURCE, CHANGELOG)
        release_notes.validate(version, SOURCE, CHANGELOG)

    def test_a_build_only_tag_validates_against_the_same_entry(self):
        version = stackcopy.STACKCOPY_VERSION
        for suffix in ("-build1", "-build2", "-build99"):
            with self.subTest(suffix=suffix):
                self.assertEqual(
                    release_notes.validate(f"v{version}{suffix}", SOURCE, CHANGELOG),
                    version,
                )

    def test_a_tag_that_disagrees_with_the_source_fails(self):
        # tag v1.5.10 + STACKCOPY_VERSION 1.5.9 -> must fail, not build.
        with self.assertRaises(release_notes.ChangelogError) as caught:
            release_notes.validate(
                "v1.5.10", 'STACKCOPY_VERSION = "1.5.9"\n', SAMPLE_CHANGELOG
            )
        message = str(caught.exception)
        self.assertIn("1.5.10", message)
        self.assertIn("1.5.9", message)

    def test_a_source_version_with_no_changelog_entry_fails(self):
        with self.assertRaises(release_notes.ChangelogError):
            release_notes.validate(
                "v9.9.9", 'STACKCOPY_VERSION = "9.9.9"\n', SAMPLE_CHANGELOG
            )

    def test_a_tag_without_a_version_number_fails(self):
        for tag in ("nightly", "v", "", "release"):
            with self.subTest(tag=tag), self.assertRaises(release_notes.ChangelogError):
                release_notes.validate(tag, SAMPLE_SOURCE, SAMPLE_CHANGELOG)

    def test_a_source_without_the_constant_fails(self):
        with self.assertRaises(release_notes.ChangelogError):
            release_notes.validate("v1.6.0", "# nothing here\n", SAMPLE_CHANGELOG)

    def test_the_readme_agrees_with_the_source_version(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn(
            f"**Version**: {stackcopy.STACKCOPY_VERSION}",
            readme,
            "README's Version section drifted from STACKCOPY_VERSION",
        )


class RenderedNotesTests(unittest.TestCase):
    def test_the_notes_are_the_changelog_section(self):
        notes = release_notes.render_notes("v1.6.0", SAMPLE_SOURCE, SAMPLE_CHANGELOG)
        self.assertIn("## Stackcopy 1.6.0", notes)
        self.assertIn("Metadata-confirmed stacks.", notes)
        self.assertIn("A crash.", notes)
        self.assertNotIn("Something not released yet", notes)
        self.assertNotIn("Date filtering", notes)

    def test_the_notes_link_back_to_the_full_changelog(self):
        notes = release_notes.render_notes("v1.6.0", SAMPLE_SOURCE, SAMPLE_CHANGELOG)
        self.assertIn(f"github.com/{updater.GITHUB_REPOSITORY}", notes)
        self.assertIn("ChangeLog.md", notes)

    def test_a_build_recut_says_it_is_not_a_new_version(self):
        notes = release_notes.render_notes(
            "v1.6.0-build2", SAMPLE_SOURCE, SAMPLE_CHANGELOG
        )
        self.assertIn("rebuild of Stackcopy 1.6.0", notes)
        self.assertIn("will not offer it as an update", notes)
        self.assertIn("## Stackcopy 1.6.0", notes)
        self.assertNotIn("Stackcopy 1.6.0-build2 is", notes)

    def test_a_plain_release_carries_no_rebuild_note(self):
        notes = release_notes.render_notes("v1.6.0", SAMPLE_SOURCE, SAMPLE_CHANGELOG)
        self.assertNotIn("rebuild", notes)

    def test_the_real_changelog_renders_notes_for_the_current_version(self):
        notes = release_notes.render_notes(
            f"v{stackcopy.STACKCOPY_VERSION}", SOURCE, CHANGELOG
        )
        self.assertIn(f"## Stackcopy {stackcopy.STACKCOPY_VERSION}", notes)
        self.assertGreater(len(notes), 200)

    def test_the_command_line_check_reports_success_and_failure(self):
        self.assertEqual(
            release_notes.main(["--check", "--tag", f"v{stackcopy.STACKCOPY_VERSION}"]),
            0,
        )
        self.assertEqual(release_notes.main(["--check", "--tag", "v99.0.0"]), 1)

    def test_the_command_line_writes_notes_to_a_file(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "notes.md"
            code = release_notes.main(
                ["--tag", f"v{stackcopy.STACKCOPY_VERSION}", "--output", str(output)]
            )
            self.assertEqual(code, 0)
            self.assertIn("## Stackcopy", output.read_text(encoding="utf-8"))


class PackagedBundleVersionTests(unittest.TestCase):
    """The PyInstaller spec must agree with the same tag rule as everything else."""

    def spec_version(self, requested=None):
        import os
        import re as regex
        from unittest import mock as umock

        source = (ROOT / "packaging" / "stackcopy_gui.spec").read_text(encoding="utf-8")
        block = source[
            source.index("def canonical_version():") : source.index(
                'print(f"Stackcopy version:'
            )
        ]
        namespace = {"os": os, "re": regex, "ROOT": str(ROOT)}
        exec(block, namespace)  # noqa: S102 - the spec is our own source
        environment = {} if requested is None else {"STACKCOPY_VERSION": requested}
        with umock.patch.dict(os.environ, environment, clear=False):
            if requested is None:
                os.environ.pop("STACKCOPY_VERSION", None)
            return namespace["canonical_version"]()

    def test_a_build_only_tag_still_builds_the_same_application_version(self):
        version = stackcopy.STACKCOPY_VERSION
        for tag in (
            f"v{version}",
            f"v{version}-build1",
            f"v{version}-build27",
            f"{version}-build3",
        ):
            with self.subTest(tag=tag):
                self.assertEqual(self.spec_version(tag), version)

    def test_a_disagreeing_tag_still_fails_the_build(self):
        with self.assertRaises(SystemExit) as caught:
            self.spec_version("v9.9.9")
        self.assertIn("9.9.9", str(caught.exception))

    def test_a_disagreeing_build_tag_also_fails_the_build(self):
        with self.assertRaises(SystemExit):
            self.spec_version("v9.9.9-build2")

    def test_the_untagged_placeholder_is_accepted(self):
        self.assertEqual(self.spec_version("0.0.0"), stackcopy.STACKCOPY_VERSION)
        self.assertEqual(self.spec_version(None), stackcopy.STACKCOPY_VERSION)

    def test_the_spec_and_the_updater_normalize_tags_identically(self):
        # One rule, used by the build gate, the release notes, and the app.
        version = stackcopy.STACKCOPY_VERSION
        self.assertEqual(updater.normalize_version(f"v{version}-build9"), version)
        self.assertEqual(self.spec_version(f"v{version}-build9"), version)
