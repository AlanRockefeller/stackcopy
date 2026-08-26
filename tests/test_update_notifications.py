"""The update *notification* system: what counts as newer, and what stays quiet.

The rule the whole feature turns on: a build number re-cuts a release, it does
not make a new one.  ``v1.5.9``, ``v1.5.9-build1`` and ``v1.5.9-build99`` are
all Stackcopy 1.5.9, and a user running 1.5.9 must never be told otherwise.

Nothing here touches the network: every check is driven through an injected
opener, so the suite is offline-safe and deterministic.
"""

import io
import json
import sys
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy_updater as updater  # noqa: E402

# ---------------------------------------------------------------------------
# Test doubles for the one HTTP call
# ---------------------------------------------------------------------------


class FakeResponse(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body if isinstance(body, bytes) else body.encode("utf-8"))
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
        return False


def release_payload(**changes):
    payload = {
        "tag_name": "v1.6.0",
        "name": "Stackcopy 1.6.0",
        "html_url": (
            "https://github.com/AlanRockefeller/stackcopy/releases/tag/v1.6.0"
        ),
        "published_at": "2026-08-24T10:00:00Z",
        "body": "### Added\n\n- Something good.\n",
        "prerelease": False,
        "draft": False,
    }
    payload.update(changes)
    return payload


def opener_for(payload, status=200):
    body = payload if isinstance(payload, (str, bytes)) else json.dumps(payload)

    def opener(_request, timeout=None):
        return FakeResponse(body, status=status)

    return opener


def failing_opener(exception):
    def opener(_request, timeout=None):
        raise exception

    return opener


# ---------------------------------------------------------------------------
# Version normalization and comparison
# ---------------------------------------------------------------------------


class VersionNormalizationTests(unittest.TestCase):
    def test_leading_v_is_stripped(self):
        self.assertEqual(updater.normalize_version("v1.5.9"), "1.5.9")
        self.assertEqual(updater.normalize_version("V1.5.9"), "1.5.9")
        self.assertEqual(updater.normalize_version("1.5.9"), "1.5.9")

    def test_build_suffixes_normalize_to_the_application_version(self):
        for tag in (
            "v1.5.9-build1",
            "v1.5.9-build2",
            "v1.5.9-build99",
            "1.5.9-build0",
            "v1.5.9_build3",
            "v1.5.9.build3",
            "v1.5.9+build3",
            "v1.5.9-BUILD7",
            "v1.5.9-Build12-retry",
        ):
            with self.subTest(tag=tag):
                self.assertEqual(updater.normalize_version(tag), "1.5.9")

    def test_surrounding_whitespace_is_ignored(self):
        self.assertEqual(updater.normalize_version("  v1.6.0-build4  "), "1.6.0")

    def test_build_metadata_is_dropped(self):
        self.assertEqual(updater.normalize_version("1.6.0+abcdef"), "1.6.0")

    def test_non_versions_are_not_invented(self):
        for value in ("", "   ", "latest", "v", "nightly", None, 17):
            with self.subTest(value=value):
                self.assertIsNone(updater.parse_version(value))

    def test_two_and_four_component_versions_parse(self):
        self.assertEqual(updater.parse_version("1.6"), (1, 6))
        self.assertEqual(updater.parse_version("v1.6.0.2"), (1, 6, 0, 2))


class VersionComparisonTests(unittest.TestCase):
    def assert_no_update(self, latest, current):
        self.assertFalse(
            updater.is_newer_version(latest, current),
            f"{latest!r} must not be offered as newer than {current!r}",
        )

    def assert_update(self, latest, current):
        self.assertTrue(
            updater.is_newer_version(latest, current),
            f"{latest!r} should be offered as newer than {current!r}",
        )

    # --- the build-number rule, stated exactly as the requirement does ---

    def test_same_version_is_not_an_update(self):
        self.assert_no_update("v1.5.9", "1.5.9")
        self.assert_no_update("1.5.9", "1.5.9")

    def test_build_only_releases_are_never_an_update(self):
        self.assert_no_update("v1.5.9-build1", "1.5.9")
        self.assert_no_update("v1.5.9-build99", "1.5.9")
        self.assert_no_update("v1.5.10-build5", "1.5.10")

    def test_a_newer_application_version_is_an_update(self):
        self.assert_update("v1.5.10", "1.5.9")
        self.assert_update("v1.5.10-build1", "1.5.9")
        self.assert_update("v1.5.10-build2", "1.5.9")

    def test_a_build_tag_updates_to_its_base_version_only(self):
        # installed 1.5.9, latest v1.5.10-build2 -> notify that 1.5.10 exists.
        self.assertEqual(updater.normalize_version("v1.5.10-build2"), "1.5.10")
        self.assert_update("v1.5.10-build2", "1.5.9")

    # --- ordinary ordering ---

    def test_minor_and_major_bumps(self):
        self.assert_update("v1.6.0", "1.5.9")
        self.assert_update("v2.0.0", "1.9.9")
        self.assert_update("v1.10.0", "1.9.0")
        self.assert_update("v1.5.10", "1.5.9")

    def test_older_releases_are_not_updates(self):
        self.assert_no_update("v1.5.8", "1.5.9")
        self.assert_no_update("v1.4.0", "1.5.9")
        self.assert_no_update("v0.9.0", "1.0.0")

    def test_missing_components_compare_as_zero(self):
        self.assert_no_update("v1.6", "1.6.0")
        self.assert_no_update("v1.6.0", "1.6")
        self.assert_update("v1.6.1", "1.6")

    def test_numeric_components_are_not_compared_as_text(self):
        self.assert_update("v1.5.10", "1.5.9")
        self.assert_no_update("v1.5.9", "1.5.10")

    def test_malformed_versions_never_trigger_an_update(self):
        for latest, current in (
            ("banana", "1.5.9"),
            ("v", "1.5.9"),
            ("", "1.5.9"),
            ("1.5.9", "banana"),
            ("1.5.9", ""),
            ("v1.6.0-rc1", "1.5.9"),
            ("v1.6.0-beta", "1.5.9"),
        ):
            with self.subTest(latest=latest, current=current):
                self.assert_no_update(latest, current)

    def test_versions_match_ignores_builds_and_padding(self):
        self.assertTrue(updater.versions_match("1.6.0", "v1.6.0-build9"))
        self.assertTrue(updater.versions_match("1.6", "1.6.0"))
        self.assertFalse(updater.versions_match("1.6.0", "1.6.1"))


# ---------------------------------------------------------------------------
# Release URL validation
# ---------------------------------------------------------------------------


class ReleaseUrlTests(unittest.TestCase):
    def test_this_projects_release_pages_are_trusted(self):
        for url in (
            "https://github.com/AlanRockefeller/stackcopy/releases",
            "https://github.com/AlanRockefeller/stackcopy/releases/latest",
            "https://github.com/AlanRockefeller/stackcopy/releases/tag/v1.6.0",
            "https://github.com/alanrockefeller/StackCopy/releases/tag/v1.6.0-build2",
            "https://www.github.com/AlanRockefeller/stackcopy/releases/tag/v1.6.0",
        ):
            with self.subTest(url=url):
                self.assertTrue(updater.is_trusted_release_url(url))

    def test_anything_else_is_rejected(self):
        for url in (
            "",
            "   ",
            None,
            42,
            # wrong scheme
            "http://github.com/AlanRockefeller/stackcopy/releases/tag/v1.6.0",
            "javascript:alert(1)",
            "file:///etc/passwd",
            "ftp://github.com/AlanRockefeller/stackcopy/releases",
            # lookalike hosts
            "https://github.com.evil.example/AlanRockefeller/stackcopy/releases",
            "https://evil.example/AlanRockefeller/stackcopy/releases",
            "https://raw.githubusercontent.com/AlanRockefeller/stackcopy/releases",
            "https://gist.github.com/AlanRockefeller/stackcopy/releases",
            # embedded credentials pointing somewhere else
            "https://github.com@evil.example/AlanRockefeller/stackcopy/releases",
            "https://user:pw@evil.example/AlanRockefeller/stackcopy/releases",
            # wrong owner or repository
            "https://github.com/EvilUser/stackcopy/releases/tag/v1.6.0",
            "https://github.com/AlanRockefeller/stackcopy-evil/releases/tag/v9",
            "https://github.com/AlanRockefeller/faststack/releases/tag/v1.6.0",
            # not a release path
            "https://github.com/AlanRockefeller/stackcopy",
            "https://github.com/AlanRockefeller/stackcopy/issues/1",
            "https://github.com/AlanRockefeller/stackcopy/releases/download/x/y.zip",
            "https://github.com/AlanRockefeller/stackcopy/releases/tag/v1/extra",
            # query and fragment smuggling
            "https://github.com/AlanRockefeller/stackcopy/releases?next=//evil",
            "https://github.com/AlanRockefeller/stackcopy/releases#@evil.example",
        ):
            with self.subTest(url=url):
                self.assertFalse(updater.is_trusted_release_url(url))

    def test_an_untrusted_url_falls_back_to_the_releases_page(self):
        self.assertEqual(
            updater.safe_release_url("https://evil.example/pwn"),
            updater.RELEASES_URL,
        )
        self.assertEqual(updater.safe_release_url(None), updater.RELEASES_URL)

    def test_a_trusted_url_is_returned_unchanged(self):
        url = "https://github.com/AlanRockefeller/stackcopy/releases/tag/v1.6.0"
        self.assertEqual(updater.safe_release_url(url), url)

    def test_the_release_page_is_never_replaced_by_api_data(self):
        info = updater.check_for_update(
            "1.5.9",
            opener=opener_for(release_payload(html_url="https://evil.example/pwn")),
        )
        self.assertEqual(info.release_url, updater.RELEASES_URL)


# ---------------------------------------------------------------------------
# Talking to GitHub
# ---------------------------------------------------------------------------


class UpdateCheckTests(unittest.TestCase):
    def test_a_valid_response_reports_a_newer_version(self):
        info = updater.check_for_update("1.5.9", opener=opener_for(release_payload()))
        self.assertTrue(info.is_newer)
        self.assertEqual(info.latest_version, "1.6.0")
        self.assertEqual(info.current_version, "1.5.9")
        self.assertEqual(info.tag_name, "v1.6.0")
        self.assertEqual(info.headline, "Stackcopy 1.6.0 is available")
        self.assertIn("Something good", info.notes)

    def test_the_same_version_is_reported_as_up_to_date(self):
        info = updater.check_for_update("1.6.0", opener=opener_for(release_payload()))
        self.assertFalse(info.is_newer)
        self.assertEqual(info.latest_version, "1.6.0")

    def test_a_build_recut_of_the_installed_version_is_not_newer(self):
        info = updater.check_for_update(
            "1.6.0", opener=opener_for(release_payload(tag_name="v1.6.0-build5"))
        )
        self.assertFalse(info.is_newer)
        self.assertEqual(info.latest_version, "1.6.0")

    def test_a_build_recut_of_a_newer_version_reports_the_base_version(self):
        info = updater.check_for_update(
            "1.5.9", opener=opener_for(release_payload(tag_name="v1.5.10-build2"))
        )
        self.assertTrue(info.is_newer)
        self.assertEqual(info.latest_version, "1.5.10")
        self.assertEqual(info.tag_name, "v1.5.10-build2")

    def test_the_request_is_unauthenticated_and_identifies_itself(self):
        seen = {}

        def opener(request, timeout=None):
            seen["url"] = request.full_url
            seen["headers"] = dict(request.header_items())
            seen["timeout"] = timeout
            return FakeResponse(json.dumps(release_payload()))

        updater.check_for_update("1.6.0", timeout=3.0, opener=opener)
        self.assertEqual(seen["url"], updater.LATEST_RELEASE_URL)
        self.assertEqual(seen["timeout"], 3.0)
        lowered = {key.lower(): value for key, value in seen["headers"].items()}
        self.assertIn("Stackcopy", lowered["user-agent"])
        self.assertNotIn("authorization", lowered)

    def test_the_default_timeout_is_short(self):
        self.assertLessEqual(updater.REQUEST_TIMEOUT_SECONDS, 10)

    # --- failures ---

    def test_a_missing_tag_is_an_error(self):
        for payload in (
            release_payload(tag_name=""),
            release_payload(tag_name=None),
            release_payload(tag_name="   "),
            {k: v for k, v in release_payload().items() if k != "tag_name"},
        ):
            with self.subTest(payload=payload), self.assertRaises(
                updater.UpdateCheckError
            ):
                updater.check_for_update("1.5.9", opener=opener_for(payload))

    def test_a_tag_without_a_version_is_an_error(self):
        with self.assertRaises(updater.UpdateCheckError):
            updater.check_for_update(
                "1.5.9", opener=opener_for(release_payload(tag_name="nightly"))
            )

    def test_malformed_json_is_an_error(self):
        with self.assertRaises(updater.UpdateCheckError) as caught:
            updater.check_for_update("1.5.9", opener=opener_for("{not json"))
        self.assertIn("could not read", str(caught.exception))

    def test_an_unexpected_response_shape_is_an_error(self):
        for body in ("[]", '"hello"', "null", "42"):
            with self.subTest(body=body), self.assertRaises(updater.UpdateCheckError):
                updater.check_for_update("1.5.9", opener=opener_for(body))

    def test_undecodable_bytes_are_an_error(self):
        with self.assertRaises(updater.UpdateCheckError):
            updater.check_for_update("1.5.9", opener=opener_for(b"\xff\xfe\x00bad"))

    def test_http_failures_are_reported_readably(self):
        error = urllib.error.HTTPError(
            updater.LATEST_RELEASE_URL, 500, "Server Error", {}, None
        )
        with self.assertRaises(updater.UpdateCheckError) as caught:
            updater.check_for_update("1.5.9", opener=failing_opener(error))
        self.assertIn("500", str(caught.exception))

    def test_a_rate_limited_check_is_reported_rather_than_crashing(self):
        error = urllib.error.HTTPError(
            updater.LATEST_RELEASE_URL, 403, "rate limited", {}, None
        )
        with self.assertRaises(updater.UpdateCheckError) as caught:
            updater.check_for_update("1.5.9", opener=failing_opener(error))
        self.assertIn("403", str(caught.exception))

    def test_a_repository_with_no_release_yet_says_so(self):
        error = urllib.error.HTTPError(
            updater.LATEST_RELEASE_URL, 404, "Not Found", {}, None
        )
        with self.assertRaises(updater.UpdateCheckError) as caught:
            updater.check_for_update("1.5.9", opener=failing_opener(error))
        self.assertIn("no published", str(caught.exception))

    def test_an_http_error_status_on_the_response_is_an_error(self):
        with self.assertRaises(updater.UpdateCheckError) as caught:
            updater.check_for_update(
                "1.5.9", opener=opener_for(release_payload(), status=503)
            )
        self.assertIn("503", str(caught.exception))

    def test_being_offline_is_an_error_not_a_crash(self):
        error = urllib.error.URLError("Name or service not known")
        with self.assertRaises(updater.UpdateCheckError) as caught:
            updater.check_for_update("1.5.9", opener=failing_opener(error))
        self.assertIn("Could not reach GitHub", str(caught.exception))

    def test_a_timeout_is_reported_as_a_timeout(self):
        for error in (
            TimeoutError("timed out"),
            urllib.error.URLError(TimeoutError("timed out")),
        ):
            with self.subTest(error=type(error).__name__):
                with self.assertRaises(updater.UpdateCheckError) as caught:
                    updater.check_for_update("1.5.9", opener=failing_opener(error))
                self.assertIn("timed out", str(caught.exception))

    def test_a_socket_error_is_an_error_not_a_crash(self):
        with self.assertRaises(updater.UpdateCheckError):
            updater.check_for_update(
                "1.5.9", opener=failing_opener(ConnectionResetError("reset"))
            )

    # --- prereleases and odd but legal payloads ---

    def test_a_prerelease_is_never_offered(self):
        info = updater.check_for_update(
            "1.5.9",
            opener=opener_for(release_payload(tag_name="v2.0.0", prerelease=True)),
        )
        self.assertFalse(info.is_newer)

    def test_a_draft_release_is_never_offered(self):
        info = updater.check_for_update(
            "1.5.9",
            opener=opener_for(release_payload(tag_name="v2.0.0", draft=True)),
        )
        self.assertFalse(info.is_newer)

    def test_missing_optional_fields_do_not_break_the_check(self):
        info = updater.check_for_update(
            "1.5.9", opener=opener_for({"tag_name": "v1.6.0"})
        )
        self.assertTrue(info.is_newer)
        self.assertEqual(info.notes, "")
        self.assertEqual(info.release_name, "v1.6.0")
        self.assertEqual(info.published_at, "")
        self.assertEqual(info.release_url, updater.RELEASES_URL)

    def test_wrongly_typed_fields_do_not_break_the_check(self):
        info = updater.check_for_update(
            "1.5.9",
            opener=opener_for(
                release_payload(
                    name=123, body=["not", "a", "string"], published_at=None
                )
            ),
        )
        self.assertTrue(info.is_newer)
        self.assertEqual(info.release_name, "v1.6.0")
        self.assertEqual(info.notes, "")

    def test_release_notes_are_trimmed_for_a_small_dialog(self):
        summary = updater.summarize_release_notes("x" * 5000, limit=100)
        self.assertLessEqual(len(summary), 100)
        self.assertTrue(summary.endswith("…"))

    def test_release_notes_keep_their_shape(self):
        summary = updater.summarize_release_notes(
            "### Added\n\n- One\n- Two\n\n\n\n### Fixed\n\n- Three\n"
        )
        self.assertEqual(summary, "### Added\n\n- One\n- Two\n\n### Fixed\n\n- Three")


# ---------------------------------------------------------------------------
# Persistence, cooldowns, skip and remind
# ---------------------------------------------------------------------------


NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)


def info_for(latest="1.6.0", current="1.5.9", is_newer=True):
    return updater.UpdateInfo(
        current_version=current,
        latest_version=latest,
        tag_name=f"v{latest}",
        release_name=f"Stackcopy {latest}",
        release_url=updater.RELEASES_URL,
        published_at="",
        notes="",
        is_newer=is_newer,
    )


class CooldownTests(unittest.TestCase):
    def test_a_first_run_checks_immediately(self):
        self.assertTrue(updater.should_check_automatically({}, NOW))

    def test_a_successful_check_is_quiet_for_24_hours(self):
        state = updater.record_success({}, NOW)
        self.assertFalse(
            updater.should_check_automatically(state, NOW + timedelta(hours=1))
        )
        self.assertFalse(
            updater.should_check_automatically(
                state, NOW + timedelta(hours=23, minutes=59)
            )
        )
        self.assertTrue(
            updater.should_check_automatically(state, NOW + timedelta(hours=24))
        )
        self.assertTrue(
            updater.should_check_automatically(state, NOW + timedelta(days=3))
        )

    def test_a_failed_check_is_retried_within_the_hour(self):
        state = updater.record_failure({}, NOW)
        self.assertFalse(
            updater.should_check_automatically(state, NOW + timedelta(minutes=30))
        )
        self.assertTrue(
            updater.should_check_automatically(state, NOW + timedelta(hours=1))
        )

    def test_the_retry_interval_is_much_shorter_than_the_success_cooldown(self):
        self.assertEqual(updater.SUCCESS_COOLDOWN_SECONDS, 24 * 60 * 60)
        self.assertEqual(updater.FAILURE_RETRY_SECONDS, 60 * 60)
        self.assertLess(updater.FAILURE_RETRY_SECONDS, updater.SUCCESS_COOLDOWN_SECONDS)

    def test_trying_is_not_the_same_as_hearing_back(self):
        # A failure must not extend the 24-hour success cooldown, and a later
        # success must clear the failure entirely.
        state = updater.record_failure({}, NOW)
        self.assertNotIn(updater.LAST_SUCCESS_KEY, state)
        self.assertIn(updater.LAST_FAILURE_KEY, state)

        state = updater.record_success(state, NOW + timedelta(minutes=5))
        self.assertIn(updater.LAST_SUCCESS_KEY, state)
        self.assertNotIn(updater.LAST_FAILURE_KEY, state)

    def test_a_failure_after_a_success_does_not_shorten_the_success_cooldown(self):
        state = updater.record_success({}, NOW)
        state = updater.record_failure(state, NOW + timedelta(minutes=1))
        # The hour-long retry has elapsed, but the daily cooldown has not.
        self.assertFalse(
            updater.should_check_automatically(state, NOW + timedelta(hours=2))
        )
        self.assertTrue(
            updater.should_check_automatically(state, NOW + timedelta(hours=25))
        )

    def test_disabling_update_checks_stops_automatic_checks(self):
        state = {updater.ENABLED_KEY: False}
        self.assertFalse(updater.should_check_automatically(state, NOW))

    def test_checks_are_enabled_by_default(self):
        self.assertTrue(updater.update_checks_enabled({}))
        self.assertTrue(updater.should_check_automatically({}, NOW))

    def test_the_old_string_spelling_of_the_flag_still_works(self):
        self.assertFalse(updater.update_checks_enabled({updater.ENABLED_KEY: "false"}))
        self.assertTrue(updater.update_checks_enabled({updater.ENABLED_KEY: "true"}))
        self.assertFalse(updater.update_checks_enabled({updater.ENABLED_KEY: "0"}))

    def test_corrupt_timestamps_mean_never_checked_rather_than_never_check(self):
        for value in ("", "not a date", [], {}, None, "2026-13-45T99:99:99"):
            with self.subTest(value=value):
                state = {updater.LAST_SUCCESS_KEY: value}
                self.assertTrue(updater.should_check_automatically(state, NOW))

    def test_a_corrupt_enabled_flag_falls_back_to_enabled(self):
        for value in ([], {}, "maybe", object()):
            with self.subTest(value=value):
                self.assertTrue(
                    updater.update_checks_enabled({updater.ENABLED_KEY: value})
                )

    def test_stored_timestamps_round_trip(self):
        stamp = updater.format_timestamp(NOW)
        self.assertEqual(updater.parse_timestamp(stamp), NOW)

    def test_a_naive_or_zulu_timestamp_is_read_as_utc(self):
        self.assertEqual(updater.parse_timestamp("2026-08-26T12:00:00"), NOW)
        self.assertEqual(updater.parse_timestamp("2026-08-26T12:00:00Z"), NOW)

    def test_an_epoch_timestamp_from_some_other_build_still_reads(self):
        self.assertEqual(updater.parse_timestamp(NOW.timestamp()), NOW)


class SkipAndRemindTests(unittest.TestCase):
    def test_a_manual_check_ignores_every_cooldown(self):
        # should_check_automatically governs the timer only; the manual path in
        # the GUI never consults it. Assert the timer would have said no.
        state = updater.record_success({}, NOW)
        self.assertFalse(updater.should_check_automatically(state, NOW))
        self.assertTrue(updater.should_notify(info_for(), state, manual=True))

    def test_skipping_persists_the_application_version(self):
        state = updater.record_skip({}, "v1.6.0-build3")
        self.assertEqual(state[updater.SKIPPED_KEY], "1.6.0")

    def test_a_skipped_version_stops_the_automatic_notification(self):
        state = updater.record_skip({}, "1.6.0")
        self.assertFalse(updater.should_notify(info_for("1.6.0"), state))

    def test_skipping_a_version_also_skips_its_build_recuts(self):
        state = updater.record_skip({}, "1.6.0")
        for tag in ("v1.6.0-build1", "v1.6.0-build2", "1.6.0-build99"):
            with self.subTest(tag=tag):
                self.assertTrue(updater.is_skipped(state, tag))
                self.assertFalse(
                    updater.should_notify(
                        info_for(updater.normalize_version(tag)), state
                    )
                )

    def test_a_newer_version_after_a_skip_notifies_again(self):
        state = updater.record_skip({}, "1.6.0")
        self.assertFalse(updater.is_skipped(state, "1.6.1"))
        self.assertTrue(updater.should_notify(info_for("1.6.1"), state))

    def test_a_manual_check_still_reports_a_skipped_version(self):
        state = updater.record_skip({}, "1.6.0")
        self.assertTrue(updater.should_notify(info_for("1.6.0"), state, manual=True))

    def test_nothing_newer_is_never_a_notification(self):
        self.assertFalse(updater.should_notify(info_for(is_newer=False), {}))
        self.assertFalse(
            updater.should_notify(info_for(is_newer=False), {}, manual=True)
        )

    def test_skipping_an_unparseable_version_stores_nothing(self):
        state = updater.record_skip({}, "")
        self.assertNotIn(updater.SKIPPED_KEY, state)

    def test_a_corrupt_skipped_field_does_not_hide_updates(self):
        for value in (None, [], {}, 17):
            with self.subTest(value=value):
                state = {updater.SKIPPED_KEY: value}
                self.assertEqual(updater.skipped_version(state), "")
                self.assertTrue(updater.should_notify(info_for(), state))

    def test_remind_me_later_persists_nothing(self):
        # "Remind Me Later" only closes the dialog, so no state key exists for
        # it and the next ordinary check can raise the same version again.
        state = {}
        self.assertTrue(updater.should_notify(info_for(), state))
        self.assertEqual(state, {})
