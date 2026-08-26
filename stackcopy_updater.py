#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Update *notification* for Stackcopy: does a newer release exist, and where.

Deliberately not an updater.  Nothing here downloads, replaces, installs, or
restarts anything - it asks the public GitHub Releases API what the newest
user-facing version is, and hands the answer back for the GUI to mention.
The only thing acted upon is a URL, and only after it has been proven to point
at this project's own release page.

Standard library only, and free of any Tk/customtkinter import, so the whole
comparison, parsing, cooldown, and URL-validation surface is testable without
a display and without a network.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

GITHUB_OWNER = "AlanRockefeller"
GITHUB_REPO = "stackcopy"
GITHUB_REPOSITORY = f"{GITHUB_OWNER}/{GITHUB_REPO}"
LATEST_RELEASE_URL = (
    f"https://api.github.com/repos/{GITHUB_REPOSITORY}/releases/latest"
)
RELEASES_URL = f"https://github.com/{GITHUB_REPOSITORY}/releases"
CHANGELOG_URL = (
    f"https://github.com/{GITHUB_REPOSITORY}/blob/main/ChangeLog.md"
)
USER_AGENT = f"Stackcopy-Update-Checker (+https://github.com/{GITHUB_REPOSITORY})"
REQUEST_TIMEOUT_SECONDS = 6.0

# A successful check is good for a day; a failed one is retried far sooner,
# because a laptop that was offline at launch is the common case and should
# not be told to wait 24 hours for an answer it never got.
SUCCESS_COOLDOWN_SECONDS = 24 * 60 * 60
FAILURE_RETRY_SECONDS = 60 * 60
# Long enough that the window is up, drawn, and interactive first.
STARTUP_DELAY_SECONDS = 2.5

# gui-state.json keys.  Named for what they mean, so "we tried" can never be
# mistaken for "we heard back".
ENABLED_KEY = "update_check_enabled"
LAST_SUCCESS_KEY = "update_last_success_at"
LAST_FAILURE_KEY = "update_last_failure_at"
SKIPPED_KEY = "update_skipped_version"

# ``1.5.9``, ``v1.5.9``, ``1.5.9-build2``, ``v1.5.9_build10`` and friends are
# all the same *application* version.  A build number re-cuts the same release
# - usually to fix the packaging, not the program - so it must never be the
# reason a user is told to update.
_BUILD_SUFFIX_RE = re.compile(r"[-_.+]?build[-_.]?\d+(?:[-_.].*)?$", re.IGNORECASE)
_NUMERIC_VERSION_RE = re.compile(r"^\d+(?:\.\d+)*$")


class UpdateCheckError(RuntimeError):
    """An update check could not be completed. Carries a short user-safe text."""


@dataclass(frozen=True)
class UpdateInfo:
    """The result of one completed check against GitHub."""

    current_version: str
    latest_version: str
    tag_name: str
    release_name: str
    release_url: str
    published_at: str
    notes: str
    is_newer: bool

    @property
    def headline(self) -> str:
        return f"Stackcopy {self.latest_version} is available"


# ---------------------------------------------------------------------------
# Version normalization and comparison
# ---------------------------------------------------------------------------


def normalize_version(version: str) -> str:
    """Reduce a tag or version string to its user-facing application version.

    ``v1.5.9-build3`` -> ``1.5.9``.  Anything that is not recognisable as a
    dotted numeric version comes back as the cleaned-up leftovers, which
    :func:`parse_version` then refuses - malformed input must not compare as
    newer than anything.
    """
    if not isinstance(version, str):
        return ""
    value = version.strip()
    if value[:1] in ("v", "V"):
        value = value[1:]
    value = value.strip()
    # Drop build metadata (``1.5.9+abcdef``) before the build-number suffix, so
    # ``1.5.9+build7`` and ``1.5.9-build7`` normalize identically.
    value = value.split("+", 1)[0]
    value = _BUILD_SUFFIX_RE.sub("", value)
    return value.strip()


def parse_version(version: str) -> tuple[int, ...] | None:
    """Return a comparable tuple, or ``None`` when the version is unusable."""
    normalized = normalize_version(version)
    if not normalized or not _NUMERIC_VERSION_RE.match(normalized):
        return None
    try:
        return tuple(int(part) for part in normalized.split("."))
    except ValueError:  # pragma: no cover - the regex already guarantees this
        return None


def _padded(left: tuple[int, ...], right: tuple[int, ...]) -> tuple[tuple, tuple]:
    """Compare ``1.6`` and ``1.6.0`` as equal rather than as different lengths."""
    width = max(len(left), len(right))
    return (
        left + (0,) * (width - len(left)),
        right + (0,) * (width - len(right)),
    )


def is_newer_version(latest: str, current: str) -> bool:
    """True only when ``latest`` is a newer *application* version.

    Build-only differences are not newer.  Unparseable versions on either side
    are not newer either: a release nobody can make sense of is not a reason to
    interrupt somebody's import.
    """
    latest_key = parse_version(latest)
    current_key = parse_version(current)
    if latest_key is None or current_key is None:
        return False
    left, right = _padded(latest_key, current_key)
    return left > right


def versions_match(left: str, right: str) -> bool:
    """True when two strings name the same application version."""
    left_key = parse_version(left)
    right_key = parse_version(right)
    if left_key is None or right_key is None:
        return normalize_version(left) == normalize_version(right)
    padded_left, padded_right = _padded(left_key, right_key)
    return padded_left == padded_right


# ---------------------------------------------------------------------------
# Release URL validation
# ---------------------------------------------------------------------------


def is_trusted_release_url(url: object) -> bool:
    """True only for an HTTPS release page of this exact repository.

    ``html_url`` arrives from the network, and the GUI hands whatever it gets
    to the user's browser.  So it is checked rather than trusted: scheme, host,
    owner, repository, and the shape of the release path all have to be right,
    and embedded credentials (``https://user@github.com/...``) fail the host
    comparison.
    """
    if not isinstance(url, str) or not url.strip():
        return False
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return False
    if parts.scheme.lower() != "https":
        return False
    if parts.netloc.lower() not in ("github.com", "www.github.com"):
        return False
    if parts.query or parts.fragment:
        return False
    segments = [segment for segment in parts.path.split("/") if segment]
    if len(segments) < 3:
        return False
    owner, repo, releases = segments[0], segments[1], segments[2]
    # GitHub treats owner/repo case-insensitively; the path words are literal.
    if owner.lower() != GITHUB_OWNER.lower() or repo.lower() != GITHUB_REPO.lower():
        return False
    if releases != "releases":
        return False
    rest = segments[3:]
    if not rest:
        return True
    if rest == ["latest"]:
        return True
    return len(rest) == 2 and rest[0] == "tag"


def safe_release_url(url: object) -> str:
    """The release URL if it can be trusted, else this project's releases page."""
    return url.strip() if is_trusted_release_url(url) else RELEASES_URL  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Release notes
# ---------------------------------------------------------------------------


def summarize_release_notes(body: object, limit: int = 1200) -> str:
    """Condense a release body into something a small dialog can show."""
    if not isinstance(body, str):
        return ""
    lines: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            if lines and lines[-1] != "":
                lines.append("")
            continue
        lines.append(line)
        if len("\n".join(lines)) >= limit:
            break
    summary = "\n".join(lines).strip()
    if len(summary) > limit:
        summary = summary[: limit - 1].rstrip() + "…"
    return summary


# ---------------------------------------------------------------------------
# The GitHub request
# ---------------------------------------------------------------------------


def fetch_latest_release(
    timeout: float = REQUEST_TIMEOUT_SECONDS,
    opener=None,
) -> dict:
    """Fetch the newest non-prerelease release. Never authenticates.

    ``opener`` exists so tests can supply a stand-in for
    ``urllib.request.urlopen`` without monkeypatching the module globally.
    """
    request = urllib.request.Request(
        LATEST_RELEASE_URL,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    open_url = opener or urllib.request.urlopen
    try:
        with open_url(request, timeout=timeout) as response:
            status = getattr(response, "status", 200) or 200
            if status >= 400:
                raise UpdateCheckError(f"GitHub returned HTTP {status}")
            raw = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise UpdateCheckError(
                "GitHub has no published Stackcopy release yet"
            ) from exc
        raise UpdateCheckError(f"GitHub returned HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        if isinstance(reason, TimeoutError):
            raise UpdateCheckError("The update check timed out") from exc
        raise UpdateCheckError(f"Could not reach GitHub: {reason}") from exc
    except TimeoutError as exc:
        raise UpdateCheckError("The update check timed out") from exc
    except OSError as exc:
        raise UpdateCheckError(f"Could not reach GitHub: {exc}") from exc

    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise UpdateCheckError(
            "GitHub returned a release response Stackcopy could not read"
        ) from exc

    if not isinstance(payload, dict):
        raise UpdateCheckError(
            "GitHub returned an unexpected release response"
        )
    return payload


def check_for_update(
    current_version: str,
    timeout: float = REQUEST_TIMEOUT_SECONDS,
    opener=None,
) -> UpdateInfo:
    """Ask GitHub for the newest release and describe it relative to ours."""
    payload = fetch_latest_release(timeout=timeout, opener=opener)

    tag_name = payload.get("tag_name")
    tag_name = tag_name.strip() if isinstance(tag_name, str) else ""
    latest_version = normalize_version(tag_name)
    if not tag_name or not latest_version:
        raise UpdateCheckError("The latest GitHub release has no version tag")
    if parse_version(latest_version) is None:
        # A tag nobody can order (``nightly``, ``v1.6.0-rc1``) is reported as a
        # failed check rather than silently compared as "not newer" - the user
        # deserves to know the answer was unusable, not just uninteresting.
        raise UpdateCheckError(
            f"The latest GitHub release tag ({tag_name!r}) is not a "
            "version Stackcopy can compare"
        )

    # /releases/latest already excludes drafts and prereleases; honour an
    # explicit flag anyway rather than depending on that behaviour alone.
    is_prerelease = bool(payload.get("prerelease")) or bool(payload.get("draft"))

    release_name = payload.get("name")
    release_name = (
        release_name.strip()
        if isinstance(release_name, str) and release_name.strip()
        else tag_name
    )
    published_at = payload.get("published_at")
    published_at = published_at.strip() if isinstance(published_at, str) else ""

    newer = (not is_prerelease) and is_newer_version(latest_version, current_version)

    return UpdateInfo(
        current_version=current_version,
        latest_version=latest_version,
        tag_name=tag_name,
        release_name=release_name,
        release_url=safe_release_url(payload.get("html_url")),
        published_at=published_at,
        notes=summarize_release_notes(payload.get("body")),
        is_newer=newer,
    )


# ---------------------------------------------------------------------------
# Persisted state: enabled, cooldowns, skipped version
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def format_timestamp(moment: datetime | None = None) -> str:
    """An ISO-8601 UTC stamp - readable to anyone who opens gui-state.json."""
    moment = moment or _utcnow()
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_timestamp(value: object) -> datetime | None:
    """Read a stored stamp. Anything unreadable simply means "never checked"."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def update_checks_enabled(state: dict, default: bool = True) -> bool:
    """Enabled unless it was explicitly turned off.

    Tolerates the old all-strings state file as well as a real JSON boolean.
    """
    value = state.get(ENABLED_KEY, None)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "yes", "1", "on"):
            return True
        if text in ("false", "no", "0", "off"):
            return False
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def skipped_version(state: dict) -> str:
    """The normalized application version the user asked not to hear about."""
    value = state.get(SKIPPED_KEY, "")
    if not isinstance(value, str):
        return ""
    return normalize_version(value)


def is_skipped(state: dict, version: str) -> bool:
    """True when this application version was skipped - build variants included."""
    skipped = skipped_version(state)
    if not skipped:
        return False
    return versions_match(skipped, version)


def next_automatic_check_due(state: dict) -> datetime | None:
    """When the next automatic check may run, or ``None`` for "right now".

    A success buys 24 hours of quiet.  A failure buys an hour, so a machine
    that happened to be offline at launch tries again the same day.  A failure
    recorded *after* the last success is what governs, and vice versa.
    """
    last_success = parse_timestamp(state.get(LAST_SUCCESS_KEY))
    last_failure = parse_timestamp(state.get(LAST_FAILURE_KEY))

    due_times: list[datetime] = []
    if last_success is not None:
        due_times.append(last_success + timedelta(seconds=SUCCESS_COOLDOWN_SECONDS))
    if last_failure is not None:
        due_times.append(last_failure + timedelta(seconds=FAILURE_RETRY_SECONDS))
    if not due_times:
        return None
    # Both cooldowns must have elapsed before another automatic check runs.
    return max(due_times)


def should_check_automatically(state: dict, now: datetime | None = None) -> bool:
    """Whether the timed background check should run at all."""
    if not update_checks_enabled(state):
        return False
    due = next_automatic_check_due(state)
    if due is None:
        return True
    return (now or _utcnow()) >= due


def should_notify(info: UpdateInfo, state: dict, manual: bool = False) -> bool:
    """Whether this result deserves the user's attention.

    A manual check always reports what it found - the user just asked.  An
    automatic one stays quiet about a version they skipped.
    """
    if not info.is_newer:
        return False
    if manual:
        return True
    return not is_skipped(state, info.latest_version)


def record_success(state: dict, now: datetime | None = None) -> dict:
    """Mark that GitHub actually answered - not merely that we asked."""
    state[LAST_SUCCESS_KEY] = format_timestamp(now)
    state.pop(LAST_FAILURE_KEY, None)
    return state


def record_failure(state: dict, now: datetime | None = None) -> dict:
    """Mark a check that did not complete, so it is retried sooner."""
    state[LAST_FAILURE_KEY] = format_timestamp(now)
    return state


def record_skip(state: dict, version: str) -> dict:
    """Skip the *application* version, so its build re-cuts stay skipped too."""
    normalized = normalize_version(version)
    if normalized:
        state[SKIPPED_KEY] = normalized
    return state
