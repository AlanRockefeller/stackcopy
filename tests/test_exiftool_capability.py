"""ExifTool capability detection, version reporting, and version visibility.

No test here needs a real ExifTool: every subprocess boundary is mocked, so
the suite says the same thing on a machine with ExifTool 12.40, one with
13.59, and one with none at all.  (TestRealExifTool at the bottom is the one
optional exception, and it skips itself when ExifTool is absent.)
"""

import io
import json
import os
import subprocess
import sys
import types
import unittest
from contextlib import ExitStack, redirect_stdout, redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stackcopy  # noqa: E402


def load_gui_module():
    """Import the GUI with stand-ins for Tk, so no display is needed."""
    try:
        import stackcopy_gui

        return stackcopy_gui
    except ImportError:
        pass

    class AnyModule(types.ModuleType):
        def __getattr__(self, name):
            if name.startswith("__"):
                raise AttributeError(name)
            placeholder = type(name, (object,), {})
            setattr(self, name, placeholder)
            return placeholder

    stubs = {
        name: AnyModule(name)
        for name in (
            "customtkinter",
            "tkinter",
            "tkinter.filedialog",
            "tkinter.messagebox",
        )
        if name not in sys.modules
    }
    with mock.patch.dict(sys.modules, stubs):
        import stackcopy_gui

        return stackcopy_gui


gui = load_gui_module()


class FakeExifTool:
    """Stand in for `exiftool -ver`, and count how often it is asked."""

    def __init__(self, stdout="13.59\n", returncode=0, stderr="", error=None):
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = stderr
        self.error = error
        self.calls: list[list[str]] = []

    def __call__(self, command, **kwargs):
        self.calls.append(list(command))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(
            returncode=self.returncode, stdout=self.stdout, stderr=self.stderr
        )


def discover(fake=None, *, which="/usr/bin/exiftool", environment=None, bundled=None):
    """Run discovery once with the outside world mocked out."""
    fake = fake or FakeExifTool()
    with ExitStack() as stack:
        stack.enter_context(
            mock.patch.dict(
                os.environ, environment or {"STACKCOPY_EXIFTOOL": ""}, clear=False
            )
        )
        stack.enter_context(
            mock.patch.object(stackcopy.shutil, "which", return_value=which)
        )
        stack.enter_context(
            mock.patch.object(stackcopy, "_bundled_exiftool_path", return_value=bundled)
        )
        stack.enter_context(mock.patch.object(stackcopy.subprocess, "run", fake))
        info = stackcopy._discover_exiftool()
    return info, fake


# ---------------------------------------------------------------------------
# Version parsing
# ---------------------------------------------------------------------------


class VersionParsingTests(unittest.TestCase):
    def test_known_versions(self):
        cases = {
            "13.59": (13, 59),
            "13.59\n": (13, 59),
            "  12.41  ": (12, 41),
            "12.40": (12, 40),
            "12.04": (12, 4),
            # ExifTool numbers are two-decimal, so 12.4 is 12.40, not 12.04.
            "12.4": (12, 40),
            "13": (13, 0),
            "13.59 (production release)": (13, 59),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(stackcopy.parse_exiftool_version(text), expected)

    def test_library_version_warning_is_extracted(self):
        cases = {
            "13.59\nWarning: Library version is 12.40": (12, 40),
            "Warning: Library version is 12.40\n13.59\n": (12, 40),
            "warning: library version is 12.41": (12, 41),
            "13.59 [Warning: Library version is 12.4]": (12, 40),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(
                    stackcopy.parse_exiftool_library_version(text), expected
                )

    def test_no_library_warning_means_none(self):
        for text in ("13.59", "13.59\n", "", None, "Warning: something else"):
            with self.subTest(text=text):
                self.assertIsNone(stackcopy.parse_exiftool_library_version(text))

    def test_nonsense_is_rejected(self):
        for text in ("", None, "unknown", "not a version", "  ", "vNext"):
            with self.subTest(text=text):
                self.assertIsNone(stackcopy.parse_exiftool_version(text))

    def test_the_floor_orders_correctly(self):
        self.assertLess(
            stackcopy.parse_exiftool_version("12.40"),
            stackcopy.EXIFTOOL_MINIMUM_OM_SYSTEM_VERSION,
        )
        for text in ("12.41", "12.42", "13.00", "13.59", "14.01"):
            with self.subTest(text=text):
                self.assertGreaterEqual(
                    stackcopy.parse_exiftool_version(text),
                    stackcopy.EXIFTOOL_MINIMUM_OM_SYSTEM_VERSION,
                )


# ---------------------------------------------------------------------------
# Capability states
# ---------------------------------------------------------------------------


class CapabilityStateTests(unittest.TestCase):
    def test_exiftool_13_59_supports_om_system(self):
        info, _fake = discover(FakeExifTool("13.59\n"))

        self.assertTrue(info.available)
        self.assertTrue(info.supports_om_system_makernotes)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.OM_SYSTEM_SUPPORTED)
        self.assertEqual(info.version, "13.59")
        self.assertEqual(info.version_tuple, (13, 59))
        self.assertEqual(
            stackcopy.exiftool_status_lines(info),
            ["ExifTool 13.59 - OM System stack metadata enabled"],
        )

    def test_exiftool_12_41_is_exactly_enough(self):
        # 12.41 is the release that added OM SYSTEM MakerNotes, so it is
        # supported, not "too old by one".
        info, _fake = discover(FakeExifTool("12.41\n"))

        self.assertTrue(info.supports_om_system_makernotes)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.OM_SYSTEM_SUPPORTED)
        self.assertIn("12.41", stackcopy.exiftool_status_lines(info)[0])

    def test_exiftool_12_40_is_too_old(self):
        info, _fake = discover(FakeExifTool("12.40\n"))

        self.assertTrue(info.available)
        self.assertFalse(info.supports_om_system_makernotes)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.TOO_OLD)
        lines = stackcopy.exiftool_status_lines(info)
        text = " ".join(lines)
        self.assertIn("12.40", text)
        self.assertIn("too old", text)
        self.assertIn("12.41", text)
        # It says what actually gets worse, and never claims detection is off.
        self.assertIn("ORF", text)
        self.assertNotIn("disabled", text.lower())

    def test_a_stale_library_shadowing_a_new_script_is_too_old(self):
        # `exiftool -ver` prints 13.59 but warns its loaded Image::ExifTool is
        # only 12.40 - the library is what parses MakerNotes, so OM SYSTEM
        # support is not actually there.
        info, _fake = discover(
            FakeExifTool(
                "13.59\n", stderr="Warning: Library version is 12.40\n"
            )
        )

        self.assertTrue(info.available)
        self.assertEqual(info.library_version, "12.40")
        self.assertEqual(info.effective_om_system_version, (12, 40))
        self.assertFalse(info.supports_om_system_makernotes)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.TOO_OLD)
        lines = stackcopy.exiftool_status_lines(info)
        text = " ".join(lines)
        self.assertIn("13.59", text)
        self.assertIn("Library version is 12.40", text)
        self.assertIn("too old", text)
        self.assertIn("ORF", text)
        self.assertNotIn("disabled", text.lower())
        payload = self.plan_payload(info)
        self.assertEqual(payload["exiftool_status"], "too_old")
        self.assertFalse(payload["exiftool_supports_om_system"])
        self.assertEqual(payload["exiftool_library_version"], "12.40")

    def test_the_warning_can_arrive_on_stdout_itself(self):
        # The real-world report: `exiftool -ver` stdout is literally
        # "13.59 [Warning: Library version is 12.40]" with nothing on stderr.
        # Old Stackcopy printed that string verbatim; the new code must parse
        # it, classify it as too old, and not echo the warning twice.
        info, _fake = discover(
            FakeExifTool("13.59 [Warning: Library version is 12.40]\n", stderr="")
        )

        self.assertEqual(info.version, "13.59")
        self.assertEqual(info.version_tuple, (13, 59))
        self.assertEqual(info.library_version, "12.40")
        self.assertTrue(info.has_library_mismatch)
        self.assertFalse(info.supports_om_system_makernotes)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.TOO_OLD)

        label = stackcopy.exiftool_version_label(info) or ""
        self.assertEqual(label, "13.59 [Warning: Library version is 12.40]")
        self.assertEqual(label.count("Warning: Library version is"), 1)

        text = " ".join(stackcopy.exiftool_status_lines(info))
        self.assertEqual(text.count("Warning: Library version is"), 1)
        self.assertIn("too old", text)

        payload = self.plan_payload(info)
        self.assertEqual(payload["exiftool_status"], "too_old")
        self.assertEqual(
            payload["exiftool_version_label"],
            "13.59 [Warning: Library version is 12.40]",
        )
        self.assertEqual(payload["exiftool_library_version"], "12.40")
        self.assertFalse(payload["exiftool_supports_om_system"])

    def test_a_clean_13_59_is_unaffected_by_the_new_check(self):
        info, _fake = discover(FakeExifTool("13.59\n", stderr=""))

        self.assertIsNone(info.library_version)
        self.assertFalse(info.has_library_mismatch)
        self.assertTrue(info.supports_om_system_makernotes)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.OM_SYSTEM_SUPPORTED)
        self.assertEqual(
            stackcopy.exiftool_status_lines(info),
            ["ExifTool 13.59 - OM System stack metadata enabled"],
        )

    def test_a_matching_library_warning_is_not_treated_as_stale(self):
        # Some builds print the line even when the versions agree.
        info, _fake = discover(
            FakeExifTool(
                "12.41\n", stderr="Warning: Library version is 12.41\n"
            )
        )

        self.assertFalse(info.has_library_mismatch)
        self.assertTrue(info.supports_om_system_makernotes)
        self.assertEqual(
            stackcopy.exiftool_status_lines(info)[0],
            "ExifTool 12.41 - OM System stack metadata enabled",
        )

    def plan_payload(self, info):
        with mock.patch.object(stackcopy, "exiftool_info", return_value=info):
            return stackcopy.exiftool_plan_status()

    def test_missing_exiftool(self):
        info, fake = discover(which=None)

        self.assertFalse(info.available)
        self.assertIsNone(info.executable)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.MISSING)
        self.assertEqual(info.source, stackcopy.ExifToolSource.NONE)
        self.assertEqual(fake.calls, [])
        text = " ".join(stackcopy.exiftool_status_lines(info))
        self.assertIn("ExifTool not found", text)
        self.assertIn("ORF", text)
        self.assertIn("12.41", text)
        self.assertNotIn("disabled", text.lower())

    def test_malformed_version_output(self):
        info, _fake = discover(FakeExifTool("banana\n"))

        self.assertFalse(info.available)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.UNUSABLE)
        self.assertIsNotNone(info.error)
        self.assertIn("banana", info.error)
        self.assertIn(
            "could not be used", " ".join(stackcopy.exiftool_status_lines(info))
        )

    def test_empty_version_output(self):
        info, _fake = discover(FakeExifTool(""))

        self.assertEqual(info.status, stackcopy.ExifToolStatus.UNUSABLE)
        self.assertIn("printed nothing", info.error)

    def test_executable_present_but_failing(self):
        info, _fake = discover(
            FakeExifTool(stdout="", returncode=2, stderr="cannot load libperl\n")
        )

        self.assertFalse(info.available)
        self.assertEqual(info.status, stackcopy.ExifToolStatus.UNUSABLE)
        self.assertIn("cannot load libperl", info.error)

    def test_executable_that_cannot_be_launched(self):
        info, _fake = discover(FakeExifTool(error=OSError(8, "Exec format error")))

        self.assertEqual(info.status, stackcopy.ExifToolStatus.UNUSABLE)
        self.assertIn("could not be run", info.error)

    def test_a_timeout_is_not_a_crash(self):
        info, _fake = discover(
            FakeExifTool(error=subprocess.TimeoutExpired("exiftool", 30))
        )

        self.assertEqual(info.status, stackcopy.ExifToolStatus.UNUSABLE)
        self.assertIn("did not answer", info.error)


# ---------------------------------------------------------------------------
# Discovery: bundled vs PATH vs override
# ---------------------------------------------------------------------------


class DiscoverySelectionTests(unittest.TestCase):
    def test_a_packaged_build_prefers_its_own_exiftool(self):
        info, fake = discover(bundled="/app/exiftool/exiftool.exe")

        self.assertEqual(info.executable, "/app/exiftool/exiftool.exe")
        self.assertEqual(info.source, stackcopy.ExifToolSource.BUNDLED)
        self.assertTrue(info.is_bundled)
        self.assertEqual(fake.calls[0][0], "/app/exiftool/exiftool.exe")
        self.assertIn("(bundled)", stackcopy.exiftool_status_lines(info)[0])

    def test_a_source_install_uses_path(self):
        info, fake = discover(bundled=None, which="/usr/bin/exiftool")

        self.assertEqual(info.executable, "/usr/bin/exiftool")
        self.assertEqual(info.source, stackcopy.ExifToolSource.PATH)
        self.assertFalse(info.is_bundled)
        self.assertNotIn("bundled", stackcopy.exiftool_status_lines(info)[0])

    def test_the_override_wins_over_both(self):
        info, fake = discover(
            bundled="/app/exiftool/exiftool.exe",
            which="/opt/custom/exiftool",
            environment={"STACKCOPY_EXIFTOOL": "/opt/custom/exiftool"},
        )

        self.assertEqual(info.executable, "/opt/custom/exiftool")
        self.assertEqual(info.source, stackcopy.ExifToolSource.OVERRIDE)

    def test_a_broken_override_is_reported_not_ignored(self):
        info, fake = discover(
            which=None, environment={"STACKCOPY_EXIFTOOL": "/nowhere/exiftool"}
        )

        self.assertFalse(info.available)
        self.assertEqual(info.source, stackcopy.ExifToolSource.OVERRIDE)
        self.assertIn("STACKCOPY_EXIFTOOL", info.error or "")
        self.assertEqual(fake.calls, [])

    def test_the_bundled_lookup_finds_a_frozen_payload(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            bundle = Path(tmp)
            (bundle / "exiftool").mkdir()
            name = "exiftool.exe" if stackcopy.IS_WINDOWS else "exiftool"
            payload = bundle / "exiftool" / name
            payload.write_text("#!/bin/sh\n")
            payload.chmod(0o755)

            with mock.patch.object(sys, "_MEIPASS", str(bundle), create=True):
                found = stackcopy._bundled_exiftool_path()

            self.assertEqual(found, str(payload))

    def test_nothing_is_bundled_in_an_ordinary_source_checkout(self):
        with ExitStack() as stack:
            for attribute in ("_MEIPASS", "frozen"):
                if hasattr(sys, attribute):
                    stack.enter_context(
                        mock.patch.object(sys, attribute, None, create=True)
                    )
            self.assertIsNone(stackcopy._bundled_exiftool_path())


class VersionIsAskedOncePerProcessTests(unittest.TestCase):
    def test_the_version_is_queried_exactly_once(self):
        fake = FakeExifTool("13.59\n")
        stackcopy.reset_exiftool_info()
        self.addCleanup(stackcopy.reset_exiftool_info)
        with ExitStack() as stack:
            stack.enter_context(
                mock.patch.dict(os.environ, {"STACKCOPY_EXIFTOOL": ""}, clear=False)
            )
            stack.enter_context(
                mock.patch.object(
                    stackcopy.shutil, "which", return_value="/usr/bin/exiftool"
                )
            )
            stack.enter_context(
                mock.patch.object(
                    stackcopy, "_bundled_exiftool_path", return_value=None
                )
            )
            stack.enter_context(mock.patch.object(stackcopy.subprocess, "run", fake))
            first = stackcopy.exiftool_info()
            for _ in range(5):
                stackcopy.exiftool_info()

        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0], ["/usr/bin/exiftool", "-ver"])
        self.assertIs(first, stackcopy.exiftool_info())

    def test_the_status_is_reported_only_once(self):
        stackcopy._exiftool_status_reported = False
        self.addCleanup(setattr, stackcopy, "_exiftool_status_reported", False)
        output = io.StringIO()
        with redirect_stdout(output):
            for _ in range(3):
                stackcopy.report_exiftool_status()

        self.assertEqual(output.getvalue().count("ExifTool"), 1)


# ---------------------------------------------------------------------------
# The reader keeps working without a suitable ExifTool
# ---------------------------------------------------------------------------


class MetadataReaderCapabilityTests(unittest.TestCase):
    def test_a_too_old_exiftool_is_still_run(self):
        # Below 12.41 an OM-1 file simply has no readable tag, but ordinary
        # Olympus bodies still work, so the fallback is what changes - not
        # whether ExifTool is consulted.
        old = stackcopy.ExifToolInfo(
            "/usr/bin/exiftool", "12.40", (12, 40), stackcopy.ExifToolSource.PATH
        )
        run = mock.Mock(
            return_value=SimpleNamespace(
                returncode=0, stdout=json.dumps([{"SourceFile": "/c/a.JPG"}])
            )
        )
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=old),
            mock.patch.object(stackcopy.subprocess, "run", run),
            redirect_stdout(io.StringIO()),
        ):
            results = stackcopy.read_stacked_image_metadata(["/c/a.JPG"])

        self.assertEqual(run.call_count, 1)
        self.assertEqual(
            results["/c/a.JPG"].state, stackcopy.StackMetadataState.UNKNOWN
        )

    def test_a_stale_library_is_run_exactly_like_that_older_version(self):
        # Policy: a script/library mismatch is treated as a real ExifTool of
        # the library's version - so a 12.40 library is still consulted (for
        # ordinary Olympus bodies) just as a real 12.40 would be, and the
        # reader never disagrees with what the status line reported.
        stale = stackcopy.ExifToolInfo(
            "/usr/bin/exiftool",
            "13.59",
            (13, 59),
            stackcopy.ExifToolSource.PATH,
            None,
            "12.40",
            (12, 40),
        )
        self.assertFalse(stale.supports_om_system_makernotes)
        run = mock.Mock(
            return_value=SimpleNamespace(
                returncode=0, stdout=json.dumps([{"SourceFile": "/c/a.JPG"}])
            )
        )
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=stale),
            mock.patch.object(stackcopy.subprocess, "run", run),
            redirect_stdout(io.StringIO()),
        ):
            results = stackcopy.read_stacked_image_metadata(["/c/a.JPG"])

        self.assertEqual(run.call_count, 1)
        self.assertEqual(
            results["/c/a.JPG"].state, stackcopy.StackMetadataState.UNKNOWN
        )

    def test_a_mismatched_but_capable_library_is_supported_and_run(self):
        # The mirror case: script 13.59, library 12.50 - the library *can*
        # read OM SYSTEM MakerNotes, so it is both reported as enabled and
        # actually consulted.  Status and execution agree.
        capable = stackcopy.ExifToolInfo(
            "/usr/bin/exiftool",
            "13.59",
            (13, 59),
            stackcopy.ExifToolSource.PATH,
            None,
            "12.50",
            (12, 50),
        )
        self.assertTrue(capable.supports_om_system_makernotes)
        self.assertEqual(
            capable.status, stackcopy.ExifToolStatus.OM_SYSTEM_SUPPORTED
        )
        run = mock.Mock(
            return_value=SimpleNamespace(
                returncode=0,
                stdout=json.dumps(
                    [{"SourceFile": "/c/a.JPG", "StackedImage": [9, 8]}]
                ),
            )
        )
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=capable),
            mock.patch.object(stackcopy.subprocess, "run", run),
            redirect_stdout(io.StringIO()),
        ):
            results = stackcopy.read_stacked_image_metadata(["/c/a.JPG"])

        self.assertEqual(run.call_count, 1)
        self.assertEqual(
            results["/c/a.JPG"].state, stackcopy.StackMetadataState.FOCUS_STACK
        )

    def test_an_unusable_exiftool_is_not_run_at_all(self):
        broken = stackcopy.ExifToolInfo(
            "/usr/bin/exiftool", None, None, stackcopy.ExifToolSource.PATH, "boom"
        )
        run = mock.Mock()
        with (
            mock.patch.object(stackcopy, "exiftool_info", return_value=broken),
            mock.patch.object(stackcopy.subprocess, "run", run),
            redirect_stdout(io.StringIO()),
        ):
            results = stackcopy.read_stacked_image_metadata(["/c/a.JPG"])

        run.assert_not_called()
        self.assertEqual(
            results["/c/a.JPG"].state, stackcopy.StackMetadataState.UNKNOWN
        )


# ---------------------------------------------------------------------------
# Machine-readable status
# ---------------------------------------------------------------------------


class PlanStatusTests(unittest.TestCase):
    def payload_for(self, info):
        with mock.patch.object(stackcopy, "exiftool_info", return_value=info):
            return stackcopy.exiftool_plan_status()

    def test_a_suitable_exiftool(self):
        payload = self.payload_for(
            stackcopy.ExifToolInfo(
                "/usr/bin/exiftool", "13.59", (13, 59), stackcopy.ExifToolSource.PATH
            )
        )

        self.assertEqual(payload["exiftool_status"], "om_system_supported")
        self.assertEqual(payload["exiftool_version"], "13.59")
        self.assertTrue(payload["exiftool_supports_om_system"])
        self.assertEqual(payload["exiftool_minimum_version"], "12.41")

    def test_an_old_exiftool(self):
        payload = self.payload_for(
            stackcopy.ExifToolInfo(
                "/usr/bin/exiftool", "12.40", (12, 40), stackcopy.ExifToolSource.PATH
            )
        )

        self.assertEqual(payload["exiftool_status"], "too_old")
        self.assertEqual(payload["exiftool_version"], "12.40")
        self.assertFalse(payload["exiftool_supports_om_system"])

    def test_a_missing_exiftool(self):
        payload = self.payload_for(
            stackcopy.ExifToolInfo(None, None, None, stackcopy.ExifToolSource.NONE)
        )

        self.assertEqual(payload["exiftool_status"], "missing")
        self.assertIsNone(payload["exiftool_version"])
        self.assertEqual(payload["exiftool_source"], "none")

    def test_a_bundled_exiftool(self):
        payload = self.payload_for(
            stackcopy.ExifToolInfo(
                "/app/exiftool/exiftool.exe",
                "13.59",
                (13, 59),
                stackcopy.ExifToolSource.BUNDLED,
            )
        )

        self.assertEqual(payload["exiftool_source"], "bundled")
        self.assertEqual(payload["exiftool_status"], "om_system_supported")

    def test_every_value_survives_a_json_round_trip(self):
        payload = self.payload_for(stackcopy.exiftool_info())
        self.assertEqual(json.loads(json.dumps(payload)), payload)


# ---------------------------------------------------------------------------
# The GUI helper, with no Tk anywhere
# ---------------------------------------------------------------------------


class GuiStatusHelperTests(unittest.TestCase):
    def display(self, **changes):
        plan = {
            "exiftool_status": "om_system_supported",
            "exiftool_version": "13.59",
            "exiftool_source": "path",
            "exiftool_minimum_version": "12.41",
        }
        plan.update(changes)
        return gui.exiftool_status_display(plan)

    def test_a_suitable_exiftool_is_quiet_and_offers_no_link(self):
        message, tone, offer = self.display()

        self.assertEqual(tone, "ok")
        self.assertFalse(offer)
        self.assertIn("13.59", message)
        self.assertIn("enabled", message)

    def test_a_bundled_exiftool_says_so(self):
        message, tone, offer = self.display(exiftool_source="bundled")

        self.assertIn("(bundled)", message)
        self.assertEqual(tone, "ok")

    def test_an_old_exiftool_warns_and_offers_the_link(self):
        message, tone, offer = self.display(
            exiftool_status="too_old", exiftool_version="12.40"
        )

        self.assertEqual(tone, "warn")
        self.assertTrue(offer)
        self.assertIn("12.40", message)
        self.assertIn("too old", message)
        self.assertIn("ORF", message)
        self.assertNotIn("disabled", message.lower())

    def test_a_missing_exiftool_warns_and_offers_the_link(self):
        message, tone, offer = self.display(
            exiftool_status="missing", exiftool_version=None
        )

        self.assertEqual(tone, "warn")
        self.assertTrue(offer)
        self.assertIn("not found", message)
        self.assertIn("12.41", message)

    def test_an_unusable_exiftool_warns(self):
        message, tone, offer = self.display(exiftool_status="unusable")

        self.assertEqual(tone, "warn")
        self.assertTrue(offer)

    def test_nothing_is_shown_without_a_plan(self):
        self.assertIsNone(gui.exiftool_status_display(None))
        self.assertIsNone(gui.exiftool_status_display({}))
        self.assertIsNone(gui.exiftool_status_display({"exiftool_status": 7}))
        self.assertIsNone(gui.exiftool_status_display({"exiftool_status": "novel"}))

    def test_the_plan_parser_keeps_the_capability_fields(self):
        payload = {
            "total": 2,
            "bytes": 10,
            "stacks": 0,
            "stacked_outputs": 0,
            "stack_inputs": 0,
            "others": 2,
            "exiftool_status": "too_old",
            "exiftool_version": "12.40",
        }
        parsed = gui.parse_plan_json(json.dumps(payload))

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["exiftool_status"], "too_old")
        self.assertEqual(gui.exiftool_status_display(parsed)[1], "warn")


# ---------------------------------------------------------------------------
# One version constant, everywhere
# ---------------------------------------------------------------------------


class VersionVisibilityTests(unittest.TestCase):
    def test_the_version_is_still_1_6_0(self):
        self.assertEqual(stackcopy.STACKCOPY_VERSION, "1.6.0")

    def test_the_gui_does_not_hard_code_its_own_copy(self):
        self.assertEqual(gui.STACKCOPY_VERSION, stackcopy.STACKCOPY_VERSION)
        self.assertEqual(gui.APP_TITLE, f"Stackcopy {stackcopy.STACKCOPY_VERSION}")

    def test_no_second_hard_coded_version_string_in_the_gui(self):
        # The GUI must read the canonical constant, not repeat the number.
        source = (ROOT / "stackcopy_gui.py").read_text(encoding="utf-8")
        self.assertNotIn(stackcopy.STACKCOPY_VERSION, source)

    def test_stackcopy_py_declares_it_exactly_once(self):
        source = (ROOT / "stackcopy.py").read_text(encoding="utf-8")
        self.assertEqual(
            source.count(f'STACKCOPY_VERSION = "{stackcopy.STACKCOPY_VERSION}"'), 1
        )

    def spec_version(self, requested=None):
        """Run the packaging spec's version logic without PyInstaller."""
        import re as regex

        source = (ROOT / "packaging" / "stackcopy_gui.spec").read_text(encoding="utf-8")
        block = source[
            source.index("def canonical_version():") : source.index(
                'print(f"Stackcopy version:'
            )
        ]
        namespace = {"os": os, "re": regex, "ROOT": str(ROOT)}
        exec(block, namespace)  # noqa: S102 - the spec is our own source
        environment = {} if requested is None else {"STACKCOPY_VERSION": requested}
        with mock.patch.dict(os.environ, environment, clear=False):
            if requested is None:
                os.environ.pop("STACKCOPY_VERSION", None)
            return namespace["canonical_version"]()

    def test_the_packaged_bundle_version_comes_from_stackcopy_py(self):
        self.assertEqual(self.spec_version(), stackcopy.STACKCOPY_VERSION)
        self.assertEqual(self.spec_version("0.0.0"), stackcopy.STACKCOPY_VERSION)
        self.assertEqual(
            self.spec_version(f"v{stackcopy.STACKCOPY_VERSION}"),
            stackcopy.STACKCOPY_VERSION,
        )

    def test_a_release_tag_that_disagrees_fails_the_build(self):
        # Tag, source, CLI, GUI and bundle metadata cannot drift apart if a
        # disagreement stops the build.
        with self.assertRaises(SystemExit) as caught:
            self.spec_version("v9.9.9")
        self.assertIn("must agree", str(caught.exception))

    def run_cli(self, args):
        completed = subprocess.run(
            [sys.executable, str(ROOT / "stackcopy.py"), *args],
            capture_output=True,
            text=True,
        )
        return completed

    def test_version_flag_still_works(self):
        completed = self.run_cli(["--version"])

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout.strip(), "Stackcopy 1.6.0")

    def test_help_shows_the_version(self):
        completed = self.run_cli(["--help"])

        self.assertEqual(completed.returncode, 0)
        self.assertIn("Stackcopy 1.6.0", completed.stdout)

    def test_no_operation_shows_the_version_with_the_help(self):
        completed = self.run_cli([])

        self.assertEqual(completed.returncode, 1)
        self.assertIn("Stackcopy 1.6.0", completed.stdout + completed.stderr)

    def test_startup_identifies_itself_on_stderr(self):
        completed = self.run_cli(["--dry-run", "--rename", str(ROOT)])

        self.assertIn("Stackcopy 1.6.0", completed.stderr)
        # stdout stays clean for pipes.
        self.assertNotIn("Stackcopy 1.6.0", completed.stdout)


# ---------------------------------------------------------------------------
# End-to-end: --plan-json stays exactly one JSON object
# ---------------------------------------------------------------------------


class PlanJsonPurityTests(unittest.TestCase):
    def plan(self, environment=None, extra_args=()):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            card.mkdir()
            (card / "P8081868.ORF").write_bytes(b"raw")
            (card / "P8081868.JPG").write_bytes(b"jpg")
            environment = dict(
                os.environ,
                STACKCOPY_LIGHTROOM_IMPORT_DIR=str(root / "Lightroom"),
                STACKCOPY_STACK_INPUT_DIR=str(root / "StackInput"),
                **(environment or {}),
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "stackcopy.py"),
                    "--lightroomimport",
                    str(card),
                    "--plan-json",
                    *extra_args,
                ],
                capture_output=True,
                text=True,
                env=environment,
            )
        return completed

    def test_stdout_is_exactly_one_json_object(self):
        completed = self.plan()

        payload = json.loads(completed.stdout)
        self.assertIsInstance(payload, dict)
        self.assertEqual(completed.stdout.count("\n"), 1)

    def test_the_version_banner_never_reaches_stdout(self):
        completed = self.plan()

        self.assertNotIn("Stackcopy 1.6.0", completed.stdout)
        self.assertIn("Stackcopy 1.6.0", completed.stderr)

    def test_exiftool_warnings_never_reach_stdout(self):
        completed = self.plan(environment={"STACKCOPY_EXIFTOOL": "/nowhere/exiftool"})

        payload = json.loads(completed.stdout)
        self.assertEqual(payload["exiftool_status"], "unusable")
        self.assertNotIn("ExifTool", completed.stdout)
        self.assertIn("ExifTool", completed.stderr)

    def test_the_plan_reports_the_version_and_the_capability(self):
        completed = self.plan()
        payload = json.loads(completed.stdout)

        self.assertEqual(payload["stackcopy_version"], "1.6.0")
        self.assertIn(
            payload["exiftool_status"],
            {"om_system_supported", "too_old", "missing", "unusable"},
        )
        self.assertEqual(payload["exiftool_minimum_version"], "12.41")

    def test_a_missing_exiftool_is_reported_as_missing(self):
        # An override pointing at nothing is the only portable way to make a
        # subprocess believe ExifTool is unusable, whatever the host has.
        completed = self.plan(environment={"STACKCOPY_EXIFTOOL": "/nowhere/exiftool"})
        payload = json.loads(completed.stdout)

        self.assertEqual(payload["exiftool_status"], "unusable")
        self.assertFalse(payload["exiftool_supports_om_system"])


class NoNagWithoutStackDetectionTests(unittest.TestCase):
    """ExifTool is irrelevant to a --no-stack-detection run, so it stays quiet.

    Being warned about a dependency the run does not use is how people learn
    to ignore warnings.
    """

    def dry_run(self, *extra_args):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            card.mkdir()
            (card / "P8081868.JPG").write_bytes(b"jpg")
            environment = dict(
                os.environ,
                STACKCOPY_LIGHTROOM_IMPORT_DIR=str(root / "Lightroom"),
                STACKCOPY_STACK_INPUT_DIR=str(root / "StackInput"),
                STACKCOPY_EXIFTOOL="/nowhere/exiftool",
                STACKCOPY_ASSUME_YES="1",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "stackcopy.py"),
                    "--lightroomimport",
                    str(card),
                    "--dry-run",
                    *extra_args,
                ],
                capture_output=True,
                text=True,
                env=environment,
            )
        return completed.stdout + completed.stderr

    def test_no_stack_detection_says_nothing_about_exiftool(self):
        self.assertNotIn("ExifTool", self.dry_run("--no-stack-detection"))

    def test_but_an_ordinary_run_does_say_something(self):
        # The positive control: the notice really is gated, not just absent.
        self.assertIn("ExifTool", self.dry_run())

    def test_plan_json_omits_exiftool_status_when_detection_is_disabled(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            card = root / "card"
            card.mkdir()
            (card / "P8081868.JPG").write_bytes(b"jpg")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "stackcopy.py"),
                    "--lightroomimport",
                    str(card),
                    "--plan-json",
                    "--no-stack-detection",
                ],
                capture_output=True,
                text=True,
                check=False,
                env=dict(
                    os.environ,
                    STACKCOPY_LIGHTROOM_IMPORT_DIR=str(root / "Lightroom"),
                    STACKCOPY_STACK_INPUT_DIR=str(root / "StackInput"),
                    STACKCOPY_EXIFTOOL="/nowhere/exiftool",
                ),
            )

        payload = json.loads(completed.stdout)
        self.assertFalse(any(key.startswith("exiftool_") for key in payload))
        self.assertNotIn("ExifTool", completed.stderr)


# ---------------------------------------------------------------------------
# Optional: the real ExifTool on this machine, if there is one
# ---------------------------------------------------------------------------


class RealExifToolIntegrationTests(unittest.TestCase):
    def test_a_real_exiftool_is_classified_consistently(self):
        import shutil as real_shutil

        executable = real_shutil.which("exiftool")
        if not executable:
            self.skipTest("no ExifTool installed")

        stackcopy.reset_exiftool_info()
        self.addCleanup(stackcopy.reset_exiftool_info)
        with mock.patch.dict(os.environ, {"STACKCOPY_EXIFTOOL": ""}, clear=False):
            info = stackcopy._discover_exiftool()

        self.assertTrue(info.available, info.error)
        self.assertIsNotNone(info.version_tuple)
        self.assertEqual(
            info.supports_om_system_makernotes,
            info.version_tuple >= stackcopy.EXIFTOOL_MINIMUM_OM_SYSTEM_VERSION,
        )
        self.assertIn(
            info.status,
            {
                stackcopy.ExifToolStatus.OM_SYSTEM_SUPPORTED,
                stackcopy.ExifToolStatus.TOO_OLD,
            },
        )


if __name__ == "__main__":
    unittest.main()
